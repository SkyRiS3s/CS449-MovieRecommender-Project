package predict

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

  val globalPred = 3.0
  val globalMae = test
    .map(r => scala.math.abs(r.rating - globalPred))
    .reduce(_ + _) / test.count.toDouble

  //MaeGlobalMethod---------------
  /** Computes the global average rating
    *
    * @return global average rating
    */
  def get_global_avg_rating(): Double = {
    return 1.0 / train.count() * train.map(r => r.rating).sum()
  }

  val global_avg_rating: Double = get_global_avg_rating()
  val global_avg_mae: Double = test
    .map(r => scala.math.abs(r.rating - global_avg_rating))
    .reduce(_ + _) / test.count.toDouble
  //------------------------------

  //MaePerUserMethod-------------------
  val user_avgRating = train.groupBy(r => r.user).map { case (k, v) =>
    (
      k,
      v.map(r => r.rating)
        .foldLeft((0.0, 1))((acc, i) =>
          ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1)
        )
        ._1
    )
  }
  val user_mae: Double = test
    .map(r =>
      (r.user, r.rating)
    ) //convert all ratings to (k,v), with k being the user_id
    .join(user_avgRating) //join on k
    .map { case (k, (v1, v2)) => scala.math.abs(v1 - v2) }
    .mean()
  //------------------------------------

  //MaePerMovieMethod------------------

  val movie_avgRating = train.groupBy(r => r.item).map { case (k, v) =>
    (
      k,
      v.map(r => r.rating)
        .foldLeft((0.0, 1))((acc, i) =>
          ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1)
        )
        ._1
    )
  }

  /** Item method MAE
    */
  val movie_mae: Double = test
    .map(r =>
      (r.item, r.rating)
    ) //convert all ratings to (k,v), with k being the movie_id
    .leftOuterJoin(
      movie_avgRating
    ) //leftouterjoin, because some movies in the test set may not have ratings in the training set
    .map {
      case (k, (v1, None))     => scala.math.abs(v1 - global_avg_rating)
      case (k, (v1, Some(v2))) => scala.math.abs(v1 - v2)
    }
    .mean()
  //------------------------------------

  //Baseline----------------------------
  /** Scale function, as described in the pdf
    *
    * @param x
    * @param r_u_point
    * @return scale(x, r_u_point)
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

  /** Normalized deviation per user and per movie in the training set
    */
  val r_hat_u_i =
    train.map(r => (r.user, (r.item, r.rating))).join(user_avgRating).map {
      case (u, ((i, r_u_i), r_u_point)) =>
        (u, i, (r_u_i - r_u_point) / scale(r_u_i, r_u_point))
    }

  /** Global average deviation per movie in the training set
    */
  val r_hat_avg_point_i = r_hat_u_i
    .groupBy { case (u, i, r) =>
      i
    }
    .map { case (i, v) =>
      (
        i,
        v.map { case (u, i, r) =>
          r
        }.sum / v.size
      )
    }

  /** Baseline MAE
    */
  val mae_baseline: Double = test
    .map(r => (r.user, (r.item, r.rating)))
    .join(user_avgRating)
    .map { case (u, ((i, r), user_avgRating)) =>
      (i, (u, r, user_avgRating))
    }
    .leftOuterJoin(r_hat_avg_point_i)
    .map {
      case (i, ((u, r, user_avgRating), Some(r_hat_avg_point_i))) =>
        scala.math.abs(
          r - (user_avgRating + r_hat_avg_point_i * scale(
            (user_avgRating + r_hat_avg_point_i),
            user_avgRating
          ))
        )
      case (i, ((u, r, user_avgRating), None)) =>
        scala.math.abs(
          r - (user_avgRating + global_avg_rating * scale(
            (user_avgRating + global_avg_rating),
            user_avgRating
          ))
        )
    }
    .mean()
  //------------------------------------------------------------------------------------------------------------------------

  //3.1.5

  /** Converts a time in nanoseconds to microseconds
    *
    * @param ns time in nanoseconds
    * @return time in microseconds
    */
  def convert_to_us(ns: Long): Double = {
    return ns.toDouble / 1000.0
  }

  /** Functions which measures the time taken for the function 'ratings_function'. It makes 'nb_trials' runs
    *
    * @param ratings_function : function whose timing is to be measured
    * @param nb_trials : nb_runs
    * @return the timings taken by the function in each run, the min of these timings, the max, the mean as well as the std
    */
  def get_timings(
      ratings_function: () => Any,
      nb_trials: Int
  ): (List[Double], Double, Double, Double, Double) = {
    assert(nb_trials > 0)
    var i = 0
    val timings = List.newBuilder[Double]
    for (i <- 1 to nb_trials) {
      val start = System.nanoTime()
      ratings_function();
      val end = System.nanoTime()
      timings += convert_to_us(end - start)
    }
    val timing_results = timings.result()
    val max: Double = timing_results.max
    val min: Double = timing_results.min
    val mean: Double = timing_results.sum * 1 / nb_trials
    val std: Double = scala.math.sqrt(
      timing_results.map(x => (x - mean) * (x - mean)).sum * 1 / nb_trials
    )

    return (timing_results, min, max, mean, std)
  }

  //-----------------------------------------------------------

  //Duration of GlobalAvgMethod

  val (
    timing_global_method,
    min_global_method,
    max_global_method,
    mean_global_method,
    std_global_method
  ) = get_timings(get_global_avg_rating, 10)

  println("\nTimings for GlobalAverageMethod : " + timing_global_method + "\n")

  //-------------------------------------------------------------

  //Duration of UserAvgMethod

  /** Method which creates a list of predictions for all 200'000 ratings, based on the user average method
    */
  def get_predictions_user_avgRating(): Unit = {
    val user_avgRating = train.groupBy(r => r.user).map { case (k, v) =>
      (
        k,
        v.map(r => r.rating)
          .foldLeft((0.0, 1))((acc, i) =>
            ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1)
          )
          ._1
      )
    }
    val ratings =
      test.map(r => (r.user, r.rating)).join(user_avgRating).map{case (u,(r,p)) => p}.sum //Remark : According to Erick's answer on Moodle, the we can use the sum as our action to compute the time
  }

  val (
    timing_user_avgRating_method,
    min_user_avgRating_method,
    max_user_avgRating_method,
    mean_user_avgRating_method,
    std_user_avgRating_method
  ) = get_timings(get_predictions_user_avgRating, 10)

  println("\nTimings for UserAverage method : " + timing_user_avgRating_method + "\n")
 
  //-------------------------------------------------------------------------

  //Duration of ItemAvgMethod

  /** Method which creates a list of predictions for all 200'000 ratings in the test set, based on the item average method
    */
  def get_predictions_movie_avgRating(): Unit = {
    val movie_avgRating = train.groupBy(r => r.item).map { case (k, v) =>
      (
        k,
        v.map(r => r.rating)
          .foldLeft((0.0, 1))((acc, i) =>
            ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1)
          )
          ._1
      )
    }

    val ratings = test
      .map(r =>
        (r.item, r.rating)
      ) //convert all ratings to (k,v), with k being the movie_id
      .leftOuterJoin(
        movie_avgRating
      ) //leftouterjoin, because some movies in the test set may not have ratings in the training set
      .map {
        case (k, (v1, None))     => global_avg_rating
        case (k, (v1, Some(v2))) => v2
      }
      .sum
  }

  val (
    timing_movie_avgRating_method,
    min_movie_avgRating_method,
    max_movie_avgRating_method,
    mean_movie_avgRating_method,
    std_movie_avgRating_method
  ) = get_timings(get_predictions_movie_avgRating, 10)

  println(
    "\nTimings for Item average method : " + timing_movie_avgRating_method + "\n"
  )

  //---------------------------------------------------------------------------

  //Duration of baseline method
  /** Method which returns a list of predictions based on the baseline methods for all 200'000 ratings in the test set
    */
  def get_predictions_baseline(): Unit = {

    val user_avgRating = train.groupBy(r => r.user).map { case (k, v) =>
      (
        k,
        v.map(r => r.rating)
          .foldLeft((0.0, 1))((acc, i) =>
            ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1)
          )
          ._1
      )
    }

    val r_hat_u_i =
      train.map(r => (r.user, (r.item, r.rating))).join(user_avgRating).map {
        case (u, ((i, r_u_i), r_u_point)) =>
          (u, i, (r_u_i - r_u_point) / scale(r_u_i, r_u_point))
      }

    val r_hat_avg_point_i = r_hat_u_i
      .groupBy { case (u, i, r) =>
        i
      }
      .map { case (i, v) =>
        (
          i,
          v.map { case (u, i, r) =>
            r
          }.sum / v.size
        )
      }

    val predictions_ratings = test
      .map(r => (r.user, (r.item, r.rating)))
      .join(user_avgRating)
      .map { case (u, ((i, r), user_avgRating)) =>
        (i, (u, r, user_avgRating))
      }
      .leftOuterJoin(r_hat_avg_point_i)
      .map {
        case (i, ((u, r, user_avgRating), Some(r_hat_avg_point_i))) =>
          user_avgRating + r_hat_avg_point_i * scale(
            (user_avgRating + r_hat_avg_point_i),
            user_avgRating
          )
        case (i, ((u, r, user_avgRating), None)) =>
          user_avgRating + global_avg_rating * scale(
            (user_avgRating + global_avg_rating),
            user_avgRating
          )

      }
      .sum
  }

  val (timing_baseline_mehthod,
    min_baseline_mehthod,
    max_baseline_mehthod,
    mean_baseline_mehthod,
    std_baseline_mehthod
  ) = get_timings(get_predictions_baseline, 10)


    println(
    "\nTimings for baseline method : " + timing_baseline_mehthod + "\n"
  )

  //-------------------------------------------------------------------------------------------------------------------------------------

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
          "Q3.1.4" -> Map(
            "MaeGlobalMethod" -> global_avg_mae, // Datatype of answer: Double
            "MaePerUserMethod" -> user_mae, // Datatype of answer: Double
            "MaePerItemMethod" -> movie_mae, // Datatype of answer: Double
            "MaeBaselineMethod" -> mae_baseline // Datatype of answer: Double
          ),
          "Q3.1.5" -> Map(
            "DurationInMicrosecForGlobalMethod" -> Map(
              "min" -> min_global_method, // Datatype of answer: Double
              "max" -> max_global_method, // Datatype of answer: Double
              "average" -> mean_global_method, // Datatype of answer: Double
              "stddev" -> std_global_method // Datatype of answer: Double
            ),
            "DurationInMicrosecForPerUserMethod" -> Map(
              "min" -> min_user_avgRating_method, // Datatype of answer: Double
              "max" -> max_user_avgRating_method, // Datatype of answer: Double
              "average" -> mean_user_avgRating_method, // Datatype of answer: Double
              "stddev" -> std_user_avgRating_method // Datatype of answer: Double
            ),
            "DurationInMicrosecForPerItemMethod" -> Map(
              "min" -> min_movie_avgRating_method, // Datatype of answer: Double
              "max" -> max_movie_avgRating_method, // Datatype of answer: Double
              "average" -> mean_movie_avgRating_method, // Datatype of answer: Double
              "stddev" -> std_movie_avgRating_method // Datatype of answer: Double
            ),
            "DurationInMicrosecForBaselineMethod" -> Map(
              "min" -> min_baseline_mehthod, // Datatype of answer: Double
              "max" -> max_baseline_mehthod, // Datatype of answer: Double
              "average" -> mean_baseline_mehthod, // Datatype of answer: Double
              "stddev" -> std_baseline_mehthod // Datatype of answer: Double
            ),
            "RatioBetweenBaselineMethodAndGlobalMethod" -> mean_baseline_mehthod/mean_global_method // Datatype of answer: Double
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
