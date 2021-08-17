package stats

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Analyzer extends App {
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

  //3.1.1
  val global_avg_rating: Double =
    1.0 / data.count() * data.map(r => r.rating).sum()

  //3.1.2

  /**
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
  val user_avgRating_max: (Int, Double) =
    user_avgRating.reduce((acc, value) => {
      if (acc._2 < value._2) value else acc
    })
  val user_avgRating_min: (Int, Double) =
    user_avgRating.reduce((acc, value) => {
      if (acc._2 >= value._2) value else acc
    })
  val user_avgRating_avg: Double =
    user_avgRating.map(_._2).sum / user_avgRating.count
  val all_users_close_to_global_avg: Boolean =
    (user_avgRating_max._2 - global_avg_rating).abs <= 0.5 && (user_avgRating_min._2 - global_avg_rating).abs <= 0.5
  val ratio: Double = user_avgRating
    .filter { case (k, v) => (v - global_avg_rating).abs <= 0.5 }
    .count()
    .toDouble / user_avgRating.count()

  //3.1.3
  val movie_avgRating = data.groupBy(r => r.item).map { case (k, v) =>
    (
      k,
      v.map(r => r.rating)
        .foldLeft((0.0, 1))((acc, i) =>
          ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1)
        )
        ._1
    )
  }
  val movie_avgRating_max: (Int, Double) =
    movie_avgRating.reduce((acc, value) => {
      if (acc._2 < value._2) value else acc
    })
  val movie_avgRating_min: (Int, Double) =
    movie_avgRating.reduce((acc, value) => {
      if (acc._2 >= value._2) value else acc
    })
  val movie_avgRating_avg: Double =
    movie_avgRating.map(_._2).sum / movie_avgRating.count
  val all_users_close_to_global_avg_movies: Boolean =
    (movie_avgRating_max._2 - global_avg_rating).abs <= 0.5 && (movie_avgRating_min._2 - global_avg_rating).abs <= 0.5
  val ratio_movie: Double = movie_avgRating
    .filter { case (k, v) => (v - global_avg_rating).abs <= 0.5 }
    .count()
    .toDouble / movie_avgRating.count()

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
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> global_avg_rating // Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
              // Using as your input data the average rating for each user,
              // report the min, max and average of the input data.
              "min" -> user_avgRating_min._2, // Datatype of answer: Double
              "max" -> user_avgRating_max._2, // Datatype of answer: Double
              "average" -> user_avgRating_avg // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> all_users_close_to_global_avg, // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> ratio // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
              // Using as your input data the average rating for each item,
              // report the min, max and average of the input data.
              "min" -> movie_avgRating_min._2, // Datatype of answer: Double
              "max" -> movie_avgRating_max._2, // Datatype of answer: Double
              "average" -> movie_avgRating_avg // Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> all_users_close_to_global_avg_movies, // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> ratio_movie // Datatype of answer: Double
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
