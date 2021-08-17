package knn

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new Conf(args)
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(test.count == 20000, "Invalid test data")

  ////////////////////////////////////////////////////////////

  /**
    * Scale function as described in Milestone 1's pdf
    *
    * @param x
    * @param r_u_point
    * @return
    */
  def scale(x: Double, r_u_point: Double): Double = {
    if (x > r_u_point) {
      return 5.0 - r_u_point
    } else if (x < r_u_point) {
      return r_u_point - 1.0
    } else {
      return 1.0
    }
  }

  /**
    * Computes the average ratings per user
    *
    * @param all_ratings - train set
    * @return RDD[(Int, Double)] where each element contains the user_id and the user's average rating score
    */
  def get_user_avgRating(all_ratings: RDD[Rating]): RDD[(Int, Double)] = {
    return all_ratings
      .map(r => (r.user, (r.rating, 1.0)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map { case (u, (s, n)) =>
        (
          u,
          s / n
        )
      }
  }

  /** Returns the normalized deviation
    *
    * @param all_ratings training set of the form : RDD[Rating]
    * @param user_avgRating  RDD which contains the user_id and the user's average rating : RDD[(Int, Double)]
    * @return  the normalized deviation : RDD[Rating], where Rating = (user_id, movie_id, normalized_rating)
    */
  def get_r_hat_u_i(
      all_ratings: RDD[Rating],
      user_avgRating: RDD[(Int, Double)]
  ): RDD[Rating] = {
    return all_ratings
      .map(r => (r.user, (r.item, r.rating)))
      .join(user_avgRating)
      .map { case (u, ((i, r_u_i), r_u_point)) =>
        Rating(u, i, (r_u_i - r_u_point) / scale(r_u_i, r_u_point))
      }
  }

  /** Preprocessing : The denominator of the preprocessing
    *
    * @param r_hat_u_i
    * @return
    */
  def get_r_u_dom(r_hat_u_i: RDD[Rating]): RDD[(Int, Double)] = {
    return r_hat_u_i.groupBy(r => r.user).map { case (k, v) =>
      (
        k,
        math.sqrt(v.map(r => r.rating * r.rating).sum)
      )
    }
  }

  /** Preprocessing
    *
    * @param r_hat_u_i
    * @param r_u_dom
    * @return
    */
  def get_r_vhat_u_i(
      r_hat_u_i: RDD[Rating],
      r_u_dom: RDD[(Int, Double)]
  ): RDD[(Int, Int, Double)] = {
    return r_hat_u_i.map(r => (r.user, (r.item, r.rating))).join(r_u_dom).map {
      case (u, ((i, r_hat_u_i), r_u_dom)) =>
        (
          u,
          i,
          r_hat_u_i / r_u_dom
        )
    }
  }

  /**
    * Baseline MAE
    */
  val baseline_mae: Double = 0.7669

  /**
    * RDD[(Int, Double)] containing the user_id and the user's average rating
    */
  val user_avgRating = get_user_avgRating(train).cache()

  /**
    * Normalized deviations for user u
    */
  val r_hat_u_i = get_r_hat_u_i(train, user_avgRating).cache()

  /**
    * Normalized deviations for user v keyed by item
    */
  val r_hat_v_i = r_hat_u_i.map(r => (r.item, (r.user, r.rating))).cache()

  /**
    * test set joined by the user_avgRating 
    */
  val test_u = test
    .map(r => (r.user, (r.item, r.rating)))
    .join(user_avgRating)

  /**
    * List of movies that are in the training set
    */
  val train_movies = r_hat_v_i.map(_._1).collect()

  /**
    * For each that has never been rated in the training set, we set the prediction equal to the user average rating, compute the mae and then compute the sum
    */
  val rest = test_u
    .filter(x => !train_movies.contains(x._2._1._1))
    .map { case (u, ((i, rui), user_avg)) =>
      scala.math.abs(rui - user_avg)
    }
    .sum

  /**
    * Test set joined by the user_avgRating and the normalized deviations for user v
    */
  val test_u_f = test_u
    .map { case (u, ((i, rui), user_avg)) =>
      (
        i,
        (u, user_avg, rui)
      )
    }
    .join(r_hat_v_i)
    .map { case (i, ((u, user_avg, rui), (v, r_hat_v_i_s))) =>
      (
        (u, v),
        (i, user_avg, rui, r_hat_v_i_s)
      )
    }

  
  /**
    * Preprocessed ratings for each user u
    */
  val r_vhat_u_i = get_r_vhat_u_i(r_hat_u_i, get_r_u_dom(r_hat_u_i))

  /**
    * Preprocessed ratings for each user v
    */
  val r_vhat_v_i = r_vhat_u_i.keyBy(x => x._2)

  /**
    * Similarities
    */
  val sims = r_vhat_u_i
    .keyBy(x => x._2)
    .join(r_vhat_v_i)
    .map { case (i, ((u, i_u, r_vhat_u_i_s), (v, i_v, r_vhat_v_i_s))) =>
      (
        (u, v),
        r_vhat_u_i_s * r_vhat_v_i_s
      )
    }
    .filter(x => x._1._1 != x._1._2)
    .reduceByKey((x, y) => x + y)

  /**
    * Finds the top k neighbours for each user u 
    *
    * @param k max number of neighbours
    * @return similarities such that we keep the top k neighbours for each user u
    */
  def compute_kbest_sims_per_user(
      k: Int
  ): RDD[((Int, Int), Double)] = {
    return sims
      .groupBy(_._1._1)
      .flatMapValues { cb =>
        cb.map(x => (x._1._2, x._2))
          .toList
          .sortBy(x => -scala.math.abs(x._2)) //Keep most significant neighbours
          .take(k)
      }
      .map { case (u, (v, suv)) =>
        ((u, v), suv)
      }
  }

  /**
    * Computes the absolute error for each rating in th test set
    *
    * @param k_best_sims similarities such that we keep the top k neighbours for each user u
    * @return RDD containing the absolute error for each rating in the test set
    */
  def maes_knn(
      k_best_sims: RDD[((Int, Int), Double)]
  ): RDD[Double] = {
    return test_u_f
      .leftOuterJoin(k_best_sims)
      .map {
        case ((u, v), ((i, user_avg, rui, r_hat_v_i_s), Some(suv))) =>
          (
            (u, i),
            (suv * r_hat_v_i_s, scala.math.abs(suv), (user_avg, rui))
          )
        case ((u, v), ((i, user_avg, rui, r_hat_v_i_s), None)) =>
          (
            (u, i),
            (0.0, 0.0, (user_avg, rui))
          )
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, (x._3._1, x._3._2)))
      .map {
        case ((u, i), (num, den, (user_avg, rui))) => {
          if (den != 0.0) {
            scala.math.abs(
              rui -
                (user_avg + (num / den) * scale(
                  (user_avg + (num / den)),
                  user_avg
                ))
            )
          } else {
            scala.math.abs(rui - user_avg)
          }
        }
      }
  }

  /**
    * Computes the MAE using the knn method
    *
    * @param k number of max neighbours
    * @return MAE using the knn method
    */
  def knn_mae(k: Int): (Double, Int) = {
    val sims = compute_kbest_sims_per_user(k)
    return (
      (maes_knn(sims).sum + rest) / (test.count().toDouble),
      sims.count().toInt
    )
  }

  //3.2.1
  val (knn_mae_10: Double, num_sims_10: Int) = knn_mae(10)
  val (knn_mae_30: Double, num_sims_30: Int) = knn_mae(30)
  val (knn_mae_50: Double, num_sims_50: Int) = knn_mae(50)
  val (knn_mae_100: Double, num_sims_100: Int) = knn_mae(100)
  val (knn_mae_200: Double, num_sims_200: Int) = knn_mae(200)
  val (knn_mae_300: Double, num_sims_300: Int) = knn_mae(300)
  val (knn_mae_400: Double, num_sims_400: Int) = knn_mae(400)
  val (knn_mae_800: Double, num_sims_800: Int) = knn_mae(800)
  val (knn_mae_943: Double, num_sims_943: Int) = knn_mae(943)

  val all_maes_list = List(
    (10, knn_mae_10),
    (30, knn_mae_30),
    (50, knn_mae_50),
    (100, knn_mae_100),
    (200, knn_mae_200),
    (300, knn_mae_300),
    (400, knn_mae_400),
    (800, knn_mae_800),
    (943, knn_mae_943)
  )

  val lowest_pair = all_maes_list
    .map(x => (x._1, x._2 - baseline_mae))
    .filter(x => x._2 < 0)
    .sortBy(x => -x._2)
    .head
  val lowest_k = lowest_pair._1
  val lowest_diff = lowest_pair._2

  //3.2.2
  def get_bytes_sims(num_sims : Int): Int = {
    return num_sims*8
  }

  val ram_size: Double = 17179869184.0

  val n_max  = ram_size.toLong / (lowest_k*8*3) 

  ////////////////////////////////////////////////

  // Save answers as JSON
  def printToFile(content: String, location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach { f =>
      try {
        f.write(content)
      } finally { f.close }
    }
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> knn_mae_10, // Datatype of answer: Double
            "MaeForK=30" -> knn_mae_30, // Datatype of answer: Double
            "MaeForK=50" -> knn_mae_50, // Datatype of answer: Double
            "MaeForK=100" -> knn_mae_100, // Datatype of answer: Double
            "MaeForK=200" -> knn_mae_200, // Datatype of answer: Double
            "MaeForK=300" -> knn_mae_300, // Datatype of answer: Double
            "MaeForK=400" -> knn_mae_400, // Datatype of answer: Double
            "MaeForK=800" -> knn_mae_800, // Datatype of answer: Double
            "MaeForK=943" -> knn_mae_943, // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> lowest_k, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> lowest_diff // Datatype of answer: Double
          ),
          "Q3.2.2" -> Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> get_bytes_sims(num_sims_10), // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> get_bytes_sims(num_sims_30), // Datatype of answer: Int
            "MinNumberOfBytesForK=50" -> get_bytes_sims(num_sims_50), // Datatype of answer: Int
            "MinNumberOfBytesForK=100" -> get_bytes_sims(num_sims_100), // Datatype of answer: Int
            "MinNumberOfBytesForK=200" -> get_bytes_sims(num_sims_200), // Datatype of answer: Int
            "MinNumberOfBytesForK=300" -> get_bytes_sims(num_sims_300), // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> get_bytes_sims(num_sims_400), // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> get_bytes_sims(num_sims_800), // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> get_bytes_sims(num_sims_943) // Datatype of answer: Int
          ),
          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> ram_size.toLong, // Datatype of answer: Long
            "MaximumNumberOfUsersThatCanFitInRam" -> n_max // Datatype of answer: Long
          )

          // Answer the Question 3.2.4 exclusively on the report.
        )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
