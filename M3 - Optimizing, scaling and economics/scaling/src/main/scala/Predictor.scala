import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg._
import breeze.numerics._
import breeze.optimize.DiffFunction.castOps

import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int]()
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String]()
  verify()
}

object Predictor {
  def main(args: Array[String]) {
    var conf = new Conf(args)

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc: SparkContext = spark.sparkContext

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- trainFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        trainBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start
    println("Read data in " + (read_duration/pow(10.0,9)) + "s")

    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()
    println("Compute kNN on train data...")

    //_----------------------------------------------------------------------
    /** Converts a time in nanoseconds to microseconds
     *
     * @param ns time in nanoseconds
     * @return time in microseconds
     */
    def convert_to_us(ns: Long): Double = ns.toDouble / 1000.0

    /** Scale function as described in the pdf of Milestone 1
     *
     * @param x rating
     * @param r_u_point user_average rating
     * @return scaled rating
     */
    def scale(x: Double, r_u_point: Double): Double = {
      if (x > r_u_point) {
        5.0 - r_u_point
      } else if (x < r_u_point) {
        r_u_point - 1.0
      } else {
        1.0
      }
    }

    /**
     * Sums all elements per row
     *
     * @param matrix mxn matrix
     * @return mx1 matrix
     */
    def sum_axis1(matrix: CSCMatrix[Double]): CSCMatrix[Double] = {
      val sv: CSCMatrix[Double] = CSCMatrix.tabulate(rows = matrix.cols, cols = 1)((_, _) => 1.0)
      matrix * sv
    }

    /**
     * Counts all elements per row
     *
     * @param matrix mxn matrix
     * @return mx1 matrix
     */
    def count_axis1(matrix: CSCMatrix[Double]): CSCMatrix[Double] = sum_axis1(matrix.mapActiveValues(_ => 1.0))

    /**
     * Computes the mean per row
     *
     * @param matrix mxn matrix
     * @return mx1 matrix
     * Remark : Notice that we not check for a division for zero, which could happen.
     * We do this since the only time we use this method is to compute the mean average of all users,
     * and since every user has rated at least one movie in the train set, we will never have a division by zero
     */
    def mean_axis1(matrix: CSCMatrix[Double]) = sum_axis1(matrix) /:/ count_axis1(matrix)

    /**
     * Computes the user average rating per user
     *
     * @param ratings_matrix train set, UxI matrix
     * @return Ux1, where n is the number of users in the train set
     */
    def get_user_avg_rating(ratings_matrix: CSCMatrix[Double]): CSCMatrix[Double] = mean_axis1(ratings_matrix)

    /**
     * Computes the normalized deviations
     *
     * @param ratings_matrix  train set, UxI matrix
     * @param user_avg_rating user average rating per user, Ux1 matrix
     * @return Normalized ratings, UxI matrix
     */
    def get_r_hat_u_i(ratings_matrix: CSCMatrix[Double], user_avg_rating: CSCMatrix[Double]): CSCMatrix[Double] = {
      val r_hat_u_i_builder: CSCMatrix.Builder[Double] = new CSCMatrix.Builder[Double](ratings_matrix.rows, ratings_matrix.cols)
      ratings_matrix.activeIterator
        .foreach { case ((row, col), rui) =>
          val user_avg: Double = user_avg_rating.apply(row, 0)
          val r_hat_u_i_s: Double = (rui - user_avg) / scale(rui, user_avg)
          r_hat_u_i_builder.add(row, col, r_hat_u_i_s)
        }
      r_hat_u_i_builder.result()
    }

    /**
     * Preprocessing ratings to compute similarities
     *
     * @param r_hat_u_i Normalized ratings; UxI matrix
     * @return matrix UxI
     */
    def get_r_vhat_u_i(r_hat_u_i: CSCMatrix[Double]): CSCMatrix[Double] = {
      val r_u_denom: CSCMatrix[Double] = sum_axis1(r_hat_u_i *:* r_hat_u_i).mapValues(x => scala.math.sqrt(x))
      val r_vhat_u_i_builder: CSCMatrix.Builder[Double] = new CSCMatrix.Builder[Double](r_hat_u_i.rows, r_hat_u_i.cols)
      r_hat_u_i.activeIterator
        .foreach { case ((row, col), r_hat_u_i_s) =>
          val denom: Double = r_u_denom.apply(row, 0)
          val r_vhat_u_i_s: Double = r_hat_u_i_s / denom
          r_vhat_u_i_builder.add(row, col, r_vhat_u_i_s)
        }
      r_vhat_u_i_builder.result()
    }


    def get_k_sims(r_vhat_u_i : CSCMatrix[Double], spark_context : SparkContext, k : Int): CSCMatrix[Double] = {
      val r_vhat_u_i_broad: Broadcast[CSCMatrix[Double]] = spark_context.broadcast(r_vhat_u_i)

      def knn(user_id : Int):  (Int, IndexedSeq[(Int, Double)]) = {
        val r_vhat_v_i_br: CSCMatrix[Double] = r_vhat_u_i_broad.value
        val user_vector: DenseVector[Double] = r_vhat_v_i_br.apply(user_id, 0 until conf_movies).t.copy //Converting to Slice vector to Dense vector with copy
        val suv_gu: DenseVector[Double] =  r_vhat_v_i_br * user_vector
        //Set suu = 0
        suv_gu.update(user_id, 0)
        (user_id, argtopk(suv_gu, k).map(v => (v, suv_gu.apply(v))))
      }

      val topks: Array[(Int, IndexedSeq[(Int, Double)])] = spark_context.parallelize(seq = 0 until conf_users).map(knn).collect()
      val k_sims_builder = new CSCMatrix.Builder[Double](conf_users, conf_users)
      topks
        .foreach(i => i._2
          .foreach(j => k_sims_builder.add(i._1, j._1, j._2)))
      k_sims_builder.result()
    }


    //_----------------------------------------------------------------------
    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- testFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        testBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close

    println("Compute predictions on test data...")
    //_----------------------------------------------------------------------

    def get_preds(test_ratings: CSCMatrix[Double], k_sims: CSCMatrix[Double], r_hat_u_i: CSCMatrix[Double], user_avg_rating: CSCMatrix[Double], spark_context: SparkContext): CSCMatrix[Double] = {
      val user_avg_broad = spark_context.broadcast(user_avg_rating)
      val k_sims_broad = spark_context.broadcast(k_sims)
      val r_hat_u_i_broad = spark_context.broadcast(r_hat_u_i)
      def get_pui(user_id : Int, movie_id : Int): (Int, Int, Double) = {
        val user_avg: Double = user_avg_broad.value.apply(user_id, 0)
        val k_sims_gu: Transpose[DenseVector[Double]] = k_sims_broad.value.apply(user_id, 0 until conf_users).t.copy.t
        val r_hat_u_i_gi: DenseVector[Double] = r_hat_u_i_broad.value.apply(0 until conf_users, movie_id).copy
        val num: Double = k_sims_gu * r_hat_u_i_gi
        val den: Double = k_sims_gu.t.mapValues(x => scala.math.abs(x)).t * r_hat_u_i_gi.mapValues(x => {
          if (x == 0) {
            0.0
          } else {
            1.0
          }
        })
        var pui: Double = 0.0
        if (den == 0) {
          pui = user_avg
        } else {
          val r_hat_avg_point_i_s: Double = num / den
          pui = user_avg + r_hat_avg_point_i_s * scale(user_avg + r_hat_avg_point_i_s, user_avg)
        }
        (user_id, movie_id, pui)
      }
      val my_seq = Seq.newBuilder[(Int, Int)]
      test_ratings.activeKeysIterator
        .foreach(x => my_seq += x)
      val preds: Array[(Int, Int, Double)] = spark_context.parallelize(my_seq.result()).map{case (u,i) => get_pui(u,i)}.collect()
      val preds_builder: CSCMatrix.Builder[Double] = new CSCMatrix.Builder[Double](conf_users, conf_movies)
      preds
        .foreach(i => preds_builder.add(i._1, i._2, i._3))
      preds_builder.result()
    }


    /**
     * Global variables to store timings
     */
    var sim_time : Double = 0.0
    var pred_time : Double = 0.0


    def get_mae(ratings_matrix : CSCMatrix[Double], test_ratings: CSCMatrix[Double], spark_context: SparkContext, k : Int) : Double = {
      //Preprocessing ratings
      val start = System.nanoTime()
      val user_avg_rating = get_user_avg_rating(ratings_matrix)
      val r_hat_u_i = get_r_hat_u_i(ratings_matrix, user_avg_rating)
      val r_vhat_u_i = get_r_vhat_u_i(r_hat_u_i)

      //Computing ratings
      val k_sims = get_k_sims(r_vhat_u_i, spark_context, k)
      sim_time = convert_to_us(System.nanoTime() - start)
      //Computing predictions
      val predictions = get_preds(test_ratings, k_sims, r_hat_u_i, user_avg_rating, spark_context)
      pred_time = convert_to_us(System.nanoTime() - start)
      //Computing MAE
      sum_axis1(sum_axis1(abs(predictions - test_ratings)).t).apply(0,0) / test_ratings.activeSize.toDouble

    }

    val mae_200 = get_mae(train, test, sc, 200) //Hardcoded for question 4.1.1
    get_mae(train, test, sc, conf_k) //to update and get the timings for 4.1.2 and 4.1.3


    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }
    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {
        var json = "";
        {
          // Limiting the scope of implicit formats with {}
          implicit val formats = org.json4s.DefaultFormats

          val answers: Map[String, Any] = Map(
            "Q4.1.1" -> Map(
              "MaeForK=200" -> mae_200  // Datatype of answer: Double
            ),
            // Both Q4.1.2 and Q4.1.3 should provide measurement only for a single run
            "Q4.1.2" ->  Map(
              "DurationInMicrosecForComputingKNN" ->   sim_time// Datatype of answer: Double
            ),
            "Q4.1.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> pred_time // Datatype of answer: Double
            )
            // Answer the other questions of 4.1.2 and 4.1.3 in your report
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
    spark.stop()
  } 
}
