package similarity

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.plans.logical.MapPartitions
import java.awt.geom.RoundRectangle2D

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

  //------------------------------------------------

  /** Converts a time in nanoseconds to microseconds
    *
    * @param ns time in nanoseconds
    * @return time in microseconds
    */
  def convert_to_us(ns: Long): Double = {
    return ns.toDouble / 1000.0
  }

  /** Scale function as described in the pdf of Milestone 1
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
    * Constructs an RDD where each entry is user and it's average rating over the all_ratings dataset
    *
    * @param all_ratings : RDD composed of Ratings -- alias for train set
    * @return RDD[(Int, Double)] where the int is the user_id and the Double is the user's average rating
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

  /** Computes the normalized deviation
    *
    * @param all_ratings : training set of the form : RDD[Rating]
    * @param user_avgRating : RDD which contains the user_id and the user's average rating : RDD[(Int, Double)]
    * @return : the normalized deviation : RDD[Rating], where Rating = (user_id, movie_id, normalized_rating)
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

  /** Computes the denominator for the preprocessing step
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

  /** Computes r_vhat_u_i (a.k.a the preprocessing part)
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
    * RDD[(Int, Double)] containing all users in the train set as well as their average rating
    */
  val user_avgRating = get_user_avgRating(train).cache()

  /**
    * Normalized deviations for each user u
    */
  val r_hat_u_i = get_r_hat_u_i(train, user_avgRating).cache()

  /**
    * Normalized ratings for each user v
    */
  val r_hat_v_i = r_hat_u_i.map(r => (r.item, (r.user, r.rating))).cache()

  /**
    * Test set joined with the user_avgRating
    * Type : RDD[(Int, ((Int, Double), Double))]
    * Element form : (user_id, ((item_id, rating_u_i), u_avgRating))
    */
  val test_u = test
    .map(r => (r.user, (r.item, r.rating)))
    .join(user_avgRating)
    .cache()

  /**
    * Variable that counts the number of similarities
    */
  var c_count: Int = 0

  /** Computes the Absolute ERROR for all movies that are not in the training set
    *
    * @param test_u : Test set joined with the user_avgRating
    * @param r_hat_v_i : Normalized deviations
    * @return MAE of all the movies that not in the training set (Double)
    */
  def rest_errs(
      test_u: RDD[(Int, ((Int, Double), Double))],
      r_hat_v_i: RDD[(Int, (Int, Double))]
  ): Double = {
    val train_movies = r_hat_v_i.map(_._1).collect()
    return test_u
      .filter(x => !train_movies.contains(x._2._1._1))
      .map { case (u, ((i, rui), user_avg)) =>
        scala.math.abs(rui - user_avg)
      }
      .sum
  }

  /**
    * Absolute error for all movies that are not in the training set
    */
  val rest = rest_errs(test_u, r_hat_v_i)

  /** Finds the cosine similarities between users
    *
    * @param r_vhat_u_i : Preprocessed ratigs
    * @return Similarities of all pair of users : RDD[((Int, Int), Double)] --> Each element has the form ((u,v), suv)
    */
  def cosine_sims(
      r_vhat_u_i: RDD[(Int, Int, Double)]
  ): (RDD[((Int, Int), Double)], Long) = {
    val start = System.nanoTime()
    val r_vhat_v_i = r_vhat_u_i.keyBy(x => x._2)
    val sims = r_vhat_u_i
      .keyBy(x => x._2)
      .join(r_vhat_v_i)
      .filter(x => x._2._1._1 < x._2._2._1)
      .map { case (i, ((u, i_u, r_vhat_u_i_s), (v, i_v, r_vhat_v_i_s))) =>
        (
          (u, v),
          r_vhat_u_i_s * r_vhat_v_i_s
        )
      }
      .reduceByKey((x, y) => x + y)
    sims.map(_._2).sum // Action to compute time required for computation
    val end = System.nanoTime()
    return (sims, end - start)
  }

  /** Compute the Absolute ERROR for each rating in the test set (except for movies that are not in the training set)
    * Remark: Size of the resulting RDD : 19968 -- The test set has 20000 ratings
    * @param test_u: test set with user average ratings
    * @param r_hat_v_i: normalized ratings
    * @param sims: similarities
    * @return Absolute error for each rating in the test set : RDD[Double]
    */
  def maes(
      test_u: RDD[(Int, ((Int, Double), Double))],
      r_hat_v_i: RDD[(Int, (Int, Double))],
      sims: RDD[((Int, Int), Double)]
  ): RDD[(Double)] = {
    return test_u
      .map { case (u, ((i, rui), user_avg)) =>
        (
          i,
          (u, user_avg, rui)
        )
      }
      .join(r_hat_v_i)
      .map {
        case (i, ((u, user_avg, rui), (v, r_hat_v_i_s))) => {
          if (u > v) {
            ((v, u), (i, user_avg, rui, r_hat_v_i_s, true))
          } else {
            ((u, v), (i, user_avg, rui, r_hat_v_i_s, false))
          }
        }
      }
      .filter(x => x._1._1 != x._1._2)
      .leftOuterJoin(sims)
      .map {
        case (
              (u, v),
              ((i, user_avg, rui, r_hat_v_i_s, swapped), Some(suv))
            ) => {
          if (swapped) {
            ((v, i), (suv * r_hat_v_i_s, scala.math.abs(suv), (user_avg, rui)))
          } else {
            ((u, i), (suv * r_hat_v_i_s, scala.math.abs(suv), (user_avg, rui)))
          }
        }
        case ((u, v), ((i, user_avg, rui, r_hat_v_i_s, swapped), None)) => {
          if (swapped) {
            ((v, i), (0.0, 0.0, (user_avg, rui)))
          } else {
            ((u, i), (0.0, 0.0, (user_avg, rui)))
          }
        }
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, (x._3._1, x._3._2)))
      .map { case ((u, i), (num, den, (user_avg, rui))) =>
        (
          scala.math.abs(
            rui - (user_avg + (num / den) * scale(
              (user_avg + (num / den)),
              user_avg
            ))
          )
        )
      }
  }

  /** Compute cosine MAE
    * Remark : It uses the mae() method
    * @param train training set
    * @param test test set
    * @return
    */
  def cosine_mae(
      train: RDD[Rating],
      test: RDD[Rating]
  ): (Double, Long) = {
    val r_vhat_u_i = get_r_vhat_u_i(r_hat_u_i, get_r_u_dom(r_hat_u_i))
    val sims_l = cosine_sims(r_vhat_u_i)
    val sims = sims_l._1
    val predictions = maes(test_u, r_hat_v_i, sims)
    return ((predictions.sum + rest) / test.count().toDouble, sims_l._2)
  }

  //2.3.4
  var min_common_movies: Double = 0.0
  var max_common_movies: Double = 0.0
  var mean_common_movies: Double = 0.0
  var std_common_movies: Double = 0.0

  /** Compute Jaccard Similarities
    *
    * @param train trining set
    * @return similarities according to the jaccard coefficient : RDD[((Int, Int), Double)] --> Element form : ((u,v), suv)
    */
  def jaccard_sims(train: RDD[Rating]): RDD[((Int, Int), Double)] = {
    val movies_per_user =
      train.map(r => (r.user, 1.0)).reduceByKey((x, y) => x + y)
    val tmp = train.map(r => (r.item, r.user))
    val common_movies = tmp
      .join(tmp)
      .filter(x => x._2._1 < x._2._2)
      .map { case (i, (u, v)) =>
        (
          (u, v),
          1.0
        )
      }
      .reduceByKey((x, y) => x + y)

    
    val mapped = common_movies.map(_._2)
    min_common_movies = mapped.min()
    max_common_movies = mapped.max()
    mean_common_movies = mapped.mean()
    std_common_movies = mapped.stdev()

    return common_movies
      .map { case ((u, v), nuv) =>
        (u, (v, nuv))
      }
      .join(movies_per_user)
      .map { case (u, ((v, nuv), nu)) =>
        (
          v,
          (u, nu, nuv)
        )
      }
      .join(movies_per_user)
      .map { case (v, ((u, nu, nuv), nv)) =>
        (
          (u, v),
          nuv / (nu + nv)
        )
      }
  }

  /** Compute Jaccard MAE
    * Remark : uses the maes() method
    * @param train training set
    * @param test test set
    * @return
    */
  def jaccard_mae(train: RDD[Rating], test: RDD[Rating]): Double = {
    val sims = jaccard_sims(train)
    c_count = sims.count().toInt
    val predictions = maes(test_u, r_hat_v_i, sims)
    return (predictions.sum + rest) / test.count().toDouble
  }


  val baseline_mae: Double = 0.7669
  //val final_cosine_mae = cosine_mae(train, test)._1

  //2.3.2 & 2.3.4
  val final_jaccard_mae = jaccard_mae(train, test)

  //2.3.3
  val num_users = user_avgRating.count.toInt
  val worst_case_count: Int = num_users * (num_users - 1) / 2

  //2.3.5
  val total_memory: Int = c_count * 8

  //2.3.1 & 2.3.6 & 2.3.7
  var final_cosine_mae: Double = 0.0
  var i = 0
  val sim_timings = List.newBuilder[Double]
  val pred_timings = List.newBuilder[Double]
  for (i <- 1 to 5) {
    val start = System.nanoTime()
    val results = cosine_mae(train, test)
    final_cosine_mae = results._1
    sim_timings += convert_to_us(results._2)
    val end = System.nanoTime()
    pred_timings += convert_to_us(end - start)
  }

  val sim_timings_results = sim_timings.result()
  val pred_timings_results = pred_timings.result()

  /**
    * Computes the min, max, mean and std of the timings passed as parameter
    *
    * @param a_list list of timings
    * @return min, max, mean and std of the timings passed as parameter
    */
  def get_timings(a_list: List[Double]): (Double, Double, Double, Double) = {
    val min = a_list.min
    val max = a_list.max
    val mean = a_list.sum / 5.0
    val std = scala.math.sqrt(
      a_list.map(x => (x - mean) * (x - mean)).sum / 5.0
    )
    return (min, max, mean, std)
  }

  val (sim_timings_min, sim_timings_max, sim_timings_mean, sim_timings_std) =
    get_timings(sim_timings_results)
  val (
    pred_timings_min,
    pred_timings_max,
    pred_timings_mean,
    pred_timings_std
  ) = get_timings(pred_timings_results)
  val average_time_per_suv = sim_timings_mean / c_count.toDouble
  val ratio_sim_pred_timings = sim_timings_mean / pred_timings_mean

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
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> final_cosine_mae, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> (final_cosine_mae - baseline_mae) // Datatype of answer: Double
          ),
          "Q2.3.2" -> Map(
            "JaccardMae" -> final_jaccard_mae, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> (final_jaccard_mae - final_cosine_mae) // Datatype of answer: Double
          ),
          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> worst_case_count // Datatype of answer: Int
          ),
          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> min_common_movies, // Datatype of answer: Double
              "max" -> max_common_movies, // Datatype of answer: Double
              "average" -> mean_common_movies, // Datatype of answer: Double
              "stddev" -> std_common_movies // Datatype of answer: Double
            )
          ),
          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> total_memory // Datatype of answer: Int
          ),
          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> pred_timings_min, // Datatype of answer: Double
              "max" -> pred_timings_max, // Datatype of answer: Double
              "average" -> pred_timings_mean, // Datatype of answer: Double
              "stddev" -> pred_timings_std // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),
          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> sim_timings_min, // Datatype of answer: Double
              "max" -> sim_timings_max, // Datatype of answer: Double
              "average" -> sim_timings_mean, // Datatype of answer: Double
              "stddev" -> sim_timings_std // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> average_time_per_suv, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> ratio_sim_pred_timings // Datatype of answer: Double
          )
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
