package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Recommender extends App {
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
  println("Loading data from: " + conf.data())
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
    val cols = l.split("\t").map(_.trim)
    Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(data.count == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal())
  val personalFile = spark.sparkContext.textFile(conf.personal())
  // TODO: Extract ratings and movie titles
  assert(personalFile.count == 1682, "Invalid personal data")

  val my_userId: Int = 944

  /** Extract ratings from CSV, includes all movies, including those non-rated --> those non-rated have NaN as rating
    */
  val personal = personalFile.map(l => {
    val cols = l.split(",", -1).map(_.trim)
    if (cols(2).nonEmpty) {
      Rating(my_userId, cols(0).toInt, cols(2).toDouble)
    } else {
      Rating(my_userId, cols(0).toInt, Double.NaN)
    }
  })

  /** Personal ratings --> user_id = 944
    */
  val personal_rated = personal.filter(r => !(r.rating.isNaN))

  /** My average rating --> average rating of user_id = 944
    */
  val my_avgRating = personal_rated.map(r => r.rating).mean()

  /** List containing all the movies I rated
    */
  val rated_movies = personal_rated.map(r => r.item).collect()

  /** Average rating per user in the dataset
    */
  val user_avgRating = data.groupBy(r => r.user).map { case (k, v) =>
    (
      k,
      v.map(r => r.rating)
        .foldLeft((0.0, 1))((acc, i) =>
          ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1)
        )
        ._1
    )
  }

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

  /** Normalized deviation
    */
  val r_hat_u_i =
    data.map(r => (r.user, (r.item, r.rating))).join(user_avgRating).map {
      case (u, ((i, r_u_i), r_u_point)) =>
        (u, i, (r_u_i - r_u_point) / scale(r_u_i, r_u_point))
    }

  /** Global average deviation
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
    .filter { case (i, r_i) =>
      !(rated_movies.contains(i))
    } //filtering out non-viewed movies

  /** Predictions of all unseen movies for me (user 944)
    * sorted by highest prediction
    */
  val predictions_i = r_hat_avg_point_i
    .map { case (i, r_i) =>
      (i, my_avgRating + r_i * scale((my_avgRating + r_i), my_avgRating))
    }
    .sortByKey(ascending = true)
    .sortBy(_._2, ascending = false)

  /** Movie mappings to convert movie id to movie title
    */
  val movie_mappings = personalFile.map(l => {
    val cols = l.split(",", -1).map(_.trim)
    (cols(0).toInt, cols(1))
  })

  assert(movie_mappings.count == 1682)

  /** Number of required recommendations
    */
  val required_recommendations: Int = 5

  /** top movies
    */
  val top_movies = predictions_i
    .filter(predictions_i.take(required_recommendations).toList.contains)
    .join(movie_mappings)
    .sortByKey(ascending = true)
    .sortBy( r=>r._2._1 , ascending = false)
    .map { case (movie_id, (prediction, movie_name)) =>
      List(movie_id, movie_name, prediction)
    }
    .collect()

  //_----------------------------------------------------



  //Bonus
  
  /**
    * Shifts numbers to a different range. If x is in [a,b], it maps it to another interval [c,d]
    *
    * @param a left of initial interval
    * @param b right of initial interval
    * @param c left of new interval
    * @param d right of new interval
    * @param t the number to be converted
    * @return the number shifted to the new interval
    */
  def shift_range(a: Double, b : Double, c:Double, d:Double, t : Double) : Double = {
    return c+((d-c)/(b-a))*(t-a)
  }
  

  /**
    * RDD[(Int, Int)] which contains the movie id and the number of ratings per movie
    */
  val ratings_per_movie = data.groupBy(r => r.item).map{case (i, c) =>
    (i, c.size)
  }

  /**
    * Defines the minimum amount of ratings for a movie to be deemed "worthy"
    */
  val k = 15.0;

  /**
    * weighted ratings per movie
    */
  val weighted_r_hat_avg_point_i = r_hat_avg_point_i.join(ratings_per_movie).map{ case (i, (r, v)) => 
    (i, shift_range(1, 10, -1, 1, ((v/(v+k))*shift_range(-1, 1, 1, 10, r) + (k/(v+k))*shift_range(1,5,1,10, 3.53))))
  }

  /**
    * Better predictions, based on weighted ratings per movie
    */
  val better_predictions_i = weighted_r_hat_avg_point_i
    .map { case (i, r_i) =>
      (i, my_avgRating + r_i * scale((my_avgRating + r_i), my_avgRating))
    }
    .sortByKey(ascending = true)
    .sortBy(_._2, ascending = false)

  val better_top_movies = better_predictions_i
    .filter(better_predictions_i.take(required_recommendations).toList.contains)
    .join(movie_mappings)
    .sortByKey(ascending = true)
    .sortBy(_._2._1, ascending = false)
    .map { case (movie_id, (prediction, movie_name)) =>
      List(movie_id, movie_name, prediction)
    }
    .collect()

//better_top_movies foreach println


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
          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-5 recommendations that have the smallest
          // movie identifier.
          "Q4.1.1" -> top_movies
          /* List[Any](
            List(
              254,
              "Batman & Robin (1997)",
              5.0
            ), // Datatypes for answer: Int, String, Double
            List(338, "Bean (1997)", 5.0),
            List(615, "39 Steps", 5.0),
            List(741, "Last Supper", 5.0),
            List(587, "Hour of the Pig", 5.0)
          ) */ ,
          "Q4.1.2 (Bonus)" -> better_top_movies 
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
