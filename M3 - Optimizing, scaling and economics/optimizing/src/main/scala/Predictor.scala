import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train: ScallopOption[String] = opt[String](required = true)
  val test: ScallopOption[String] = opt[String](required = true)
  val k: ScallopOption[Int] = opt[Int]()
  val json: ScallopOption[String] = opt[String]()
  val users: ScallopOption[Int] = opt[Int]()
  val movies: ScallopOption[Int] = opt[Int]()
  val separator: ScallopOption[String] = opt[String]()
  verify()
}

object Predictor {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    val conf = new Conf(args)

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows = conf.users(), cols = conf.movies())
    for (line <- trainFile.getLines) {
      val cols = line.split(conf.separator()).map(_.trim)
      trainBuilder.add(cols(0).toInt - 1, cols(1).toInt - 1, cols(2).toDouble)
    }
    val train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start

    println("Read data in " + (read_duration / pow(10.0, 9)) + "s")

    println("Compute kNN on train data...")

    //----------------------------------------------------------------

    /** Converts a time in nanoseconds to microseconds
     *
     * @param ns time in nanoseconds
     * @return time in microseconds
     */
    def convert_to_us(ns: Long): Double = ns.toDouble / 1000.0

    /**
     * Gets the required results from the timings provided
     *
     * @param a_list list of measured times
     * @return
     */
    def get_timings(a_list: List[Double]): (Double, Double, Double, Double) = {
      val min: Double = a_list.min
      val max: Double = a_list.max
      val mean: Double = a_list.sum / a_list.size.toDouble
      val std: Double = scala.math.sqrt(
        a_list.map(x => (x - mean) * (x - mean)).sum / a_list.size.toDouble
      )
      (min, max, mean, std)
    }

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

    /**
     * Computes the KNN
     *
     * @param k number of neighbours
     * @param r_vhat_u_i preprocessed ratings
     * @return top k similarities; UxU matrix, which each row has at most k active values
     */
    def get_k_sims(k: Int, r_vhat_u_i: CSCMatrix[Double]): CSCMatrix[Double] = {
      //Compute all sims
      val sims: CSCMatrix[Double] = r_vhat_u_i * r_vhat_u_i.t

      //Fill diagonal with zeroes
      sims.activeKeysIterator
        .foreach { case (row, col) => if (row == col) sims.update(row, col, 0.0)
        }

      if (conf.users() == k) {
        return sims
      }

      //Keep k best sims per user u
      val k_sims_builder = new CSCMatrix.Builder[Double](sims.rows, sims.cols)
      (0 until sims.rows)
        .foreach(i => argtopk(sims(i, 0 until sims.cols).t, k) foreach { j =>
          k_sims_builder.add(i, j, sims.apply(i, j))
        })
      k_sims_builder.result()
    }

    /**
     * User average ratings
     */
    val user_avg_rating: CSCMatrix[Double] = get_user_avg_rating(train)

    /**
     * Normalized ratings
     */
    val r_hat_u_i: CSCMatrix[Double] = get_r_hat_u_i(train, user_avg_rating)

    /**
     * Preprocessed ratings
     */
    val r_vhat_u_i: CSCMatrix[Double] = get_r_vhat_u_i(r_hat_u_i)

    /**
     * KNN for k = 200
     */
    val k_sims_200: CSCMatrix[Double] = get_k_sims(k = 200, r_vhat_u_i)

    /**
     * KNN for k = 100
     */
    val k_sims_100: CSCMatrix[Double] = get_k_sims(k = 100, r_vhat_u_i)



    //-----------------------------------------------------------------

    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows = conf.users(), cols = conf.movies())
    for (line <- testFile.getLines) {
      val cols = line.split(conf.separator()).map(_.trim)
      testBuilder.add(cols(0).toInt - 1, cols(1).toInt - 1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close
    println("Compute predictions on test data...")

    //-----------------------------------------------------------------

    /**
     * Computes the predictions and the mae on the test set
     *
     * @param k_sims          top k similarities per user ; UxU matrix
     * @param r_hat_u_i       normalized ratings ; UxI matrix
     * @param user_avg_rating user average ratings ; Ux1 matrix
     * @param test_ratings    test set
     * @return mae on the test set
     */
    def get_mae(k_sims: CSCMatrix[Double], r_hat_u_i: CSCMatrix[Double], user_avg_rating: CSCMatrix[Double], test_ratings: CSCMatrix[Double]): Double = {
      val diff_builder = new CSCMatrix.Builder[Double](rows = test.rows, cols = test.cols)
      test_ratings.activeIterator
        .foreach { case ((row, col), rui) =>
          val user_avg: Double = user_avg_rating.apply(row, 0)
          val k_sims_gu: Transpose[DenseVector[Double]] = k_sims(row, 0 until k_sims.cols).t.copy.t //Converting SliceVector to DenseVector
          val r_hat_u_i_gi: DenseVector[Double] = r_hat_u_i(0 until r_hat_u_i.rows, col).copy //Converting SliceVector to DenseVector
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

          diff_builder.add(row, col, scala.math.abs(rui - pui))
        }
      sum_axis1(sum_axis1(diff_builder.result).t).apply(0, 0) / test_ratings.activeSize.toDouble
    }

    val mae_200: Double = get_mae(k_sims_200, r_hat_u_i, user_avg_rating, test)
    val mae_100: Double = get_mae(k_sims_100, r_hat_u_i, user_avg_rating, test)


    val conf_k: Int = conf.k()

    println("Computing knn for k = " + conf_k.toString + " on train data (5 times)")
    val sim_timings = List.newBuilder[Double]
    for (_ <- 1 to 5) {
      val start = System.nanoTime()
      val user_avg_rating = get_user_avg_rating(train)
      val r_hat_u_i = get_r_hat_u_i(train, user_avg_rating)
      val r_vhat_u_i = get_r_vhat_u_i(r_hat_u_i)
      get_k_sims(conf_k, r_vhat_u_i)
      val end = System.nanoTime()
      sim_timings += convert_to_us(end-start)
    }

    println("Computing predictions for k = " + conf_k.toString + " on test set (5 times)")
    val pred_timings = List.newBuilder[Double]
    for (_ <- 1 to 5) {
      val start = System.nanoTime()
      val user_avg_rating = get_user_avg_rating(train)
      val r_hat_u_i = get_r_hat_u_i(train, user_avg_rating)
      val r_vhat_u_i = get_r_vhat_u_i(r_hat_u_i)
      get_mae(get_k_sims(conf_k, r_vhat_u_i), r_hat_u_i, user_avg_rating, test)
      val end = System.nanoTime()
      pred_timings += convert_to_us(end-start)
    }

    val t_sims = get_timings(sim_timings.result())
    val t_preds = get_timings(pred_timings.result())

    //-----------------------------------------------------------------
    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json"): Unit =
      Some(new java.io.PrintWriter(location)).foreach {
        f =>
          try {
            f.write(content)
          } finally {
            f.close()
          }
      }

    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {
        var json = "";
        {
          // Limiting the scope of implicit formats with {}
          implicit val formats = org.json4s.DefaultFormats

          val answers: Map[String, Any] = Map(
            "Q3.3.1" -> Map(
              "MaeForK=100" -> mae_100, // Datatype of answer: Double
              "MaeForK=200" -> mae_200 // Datatype of answer: Double
            ),
            "Q3.3.2" -> Map(
              "DurationInMicrosecForComputingKNN" -> Map(
                "min" -> t_sims._1, // Datatype of answer: Double
                "max" -> t_sims._2, // Datatype of answer: Double
                "average" -> t_sims._3, // Datatype of answer: Double
                "stddev" -> t_sims._4 // Datatype of answer: Double
              )
            ),
            "Q3.3.3" -> Map(
              "DurationInMicrosecForComputingPredictions" -> Map(
                "min" -> t_preds._1, // Datatype of answer: Double
                "max" -> t_preds._2, // Datatype of answer: Double
                "average" -> t_preds._3, // Datatype of answer: Double
                "stddev" -> t_preds._4 // Datatype of answer: Double
              )
            )
            // Answer the Question 3.3.4 exclusively on the report.
          )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  }
}
