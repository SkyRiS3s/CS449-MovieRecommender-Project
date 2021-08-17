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

  /** My personal user_id
    */
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

  /** Computes the average rating per user in the dataset passed as parameter
    *
    * @param all_ratings alias for train set
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

  /** RDD[(Int, String)] which contains the title of a movie by the movie's id
    */
  val movie_mappings = personalFile.map(l => {
    val cols = l.split(",", -1).map(_.trim)
    (cols(0).toInt, cols(1))
  })

  /** My Personal ratings (i.e. Ratings for user with user_id = 944)
    */
  val personal_rated = personal.filter(r => !(r.rating.isNaN))

  /** Non rated movies
    */
  val personal_non_rated = personal
    .filter(r => r.rating.isNaN)
    .map(x => (x.item, x.user))

  /** My average rating --> average rating of user_id = 944
    */
  val my_avgRating = personal_rated.map(r => r.rating).mean()

  /** Normalized ratings for user 944
    */
  val r_hat_944_i = personal_rated.map(r =>
    Rating(
      r.user,
      r.item,
      (r.rating - my_avgRating) / scale(r.rating, my_avgRating)
    )
  )

  /** RDD[(Int, Double)] containing all users in the train set as well as their average rating
    */
  val user_avgRating = get_user_avgRating(data)
  val r_hat_v_i =
    get_r_hat_u_i(data, user_avgRating).map(r => (r.item, (r.user, r.rating)))

  val test_u_f = personal_non_rated.join(r_hat_v_i).map {
    case (i, (my_u, (v, r_hat_v_i_s))) =>
      (
        v,
        (i, r_hat_v_i_s)
      )
  }

  /** Preprocessed ratings for user 944
    */
  val r_vhat_944_i = get_r_vhat_u_i(r_hat_944_i, get_r_u_dom(r_hat_944_i))

  /** Normalized deviations for each user v
    */
  val r_hat_v_i_new = get_r_hat_u_i(data, user_avgRating)

  /** Preprocessed ratings for each user v
    */
  val r_vhat_v_i =
    get_r_vhat_u_i(r_hat_v_i_new, get_r_u_dom(r_hat_v_i_new)).keyBy(x => x._2)

  /** Similarities between user 944 and all other users
    */
  val sims = r_vhat_944_i
    .keyBy(x => x._2)
    .join(r_vhat_v_i)
    .map { case (i, ((u, i_u, r_vhat_u_i_s), (v, i_v, r_vhat_v_i_s))) =>
      (
        v,
        r_vhat_u_i_s * r_vhat_v_i_s
      )
    }
    .reduceByKey((x, y) => (x + y))
    .sortBy(x => -scala.math.abs(x._2))

  /** Finds the top 5 films using the knn method
    *
    * @param k max number of neighbours
    * @return the top 5 film
    */
  def top_films(k: Int): List[Any] = {
    val k_best_sims = sims.zipWithIndex
      .filter(x => x._2 < k)
      .map(x => x._1)

    val top_preds = test_u_f
      .leftOuterJoin(k_best_sims)
      .map {
        case (v, ((i, r_hat_v_i_s), Some(suv))) =>
          (
            i,
            (suv * r_hat_v_i_s, scala.math.abs(suv))
          )
        case (v, ((i, r_hat_v_i_s), None)) =>
          (
            i,
            (0.0, 0.0)
          )
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map {
        case (i, (num, den)) => {
          if (den != 0) {
            (
              i,
              my_avgRating + (num / den) * scale(
                (my_avgRating + (num / den)),
                my_avgRating
              )
            )
          } else {
            (i, my_avgRating)
          }
        }
      }
      .sortByKey(ascending = true)
      .sortBy(_._2, ascending = false)
      .zipWithIndex
      .filter(x => x._2 < 5)
      .map(x => x._1)

    val top_movies = top_preds
      .join(movie_mappings)
      .sortByKey(ascending = true)
      .sortBy(x => x._2._1, ascending = false)
      .map { case (i, (rating, movie_name)) =>
        List(i, movie_name, rating)
      }

    return top_movies.collect().toList

  }

  val top_movies_k30 = top_films(30)
  val top_movies_k300 = top_films(300)

  /** Finds the top 5 films using the knn method
    * Uses IMDB's weighting formula to give more weight to movies that have been rated more often
    * @param k
    * @return
    */
  def top_films_imdb(k: Int): List[Any] = {

    /** where the number t is mapped from an interval [a,b] to an interval [c,d]
      *
      * @param a
      * @param b
      * @param c
      * @param d
      * @param t
      * @return
      */
    def shift_range(
        a: Double,
        b: Double,
        c: Double,
        d: Double,
        t: Double
    ): Double = {
      return c + ((d - c) / (b - a)) * (t - a)
    }

    val ratings_per_movie = data
      .map(r => (r.item, 1.0))
      .reduceByKey((x, y) => x + y)

    val k_imdb = 30.0

    val k_best_sims = sims.zipWithIndex
      .filter(x => x._2 < k)
      .map(x => x._1)

    val top_preds = test_u_f
      .leftOuterJoin(k_best_sims)
      .map {
        case (v, ((i, r_hat_v_i_s), Some(suv))) =>
          (
            i,
            (suv * r_hat_v_i_s, scala.math.abs(suv))
          )
        case (v, ((i, r_hat_v_i_s), None)) =>
          (
            i,
            (0.0, 0.0)
          )
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .join(ratings_per_movie)
      .map {
        case (i, ((num, den), movie_count)) => {
          if (den != 0) {
            val r_hat_avg_point_i = num / den
            val r_i = shift_range(
              1,
              10,
              -1,
              1,
              ((movie_count / (movie_count + k_imdb)) * shift_range(
                -1,
                1,
                1,
                10,
                r_hat_avg_point_i
              ) + (k_imdb / (movie_count + k_imdb)) * shift_range(
                1,
                5,
                1,
                10,
                3.53
              ))
            )
            (
              i,
              my_avgRating + r_i * scale((my_avgRating + r_i), my_avgRating)
            )
          } else {
            (i, my_avgRating)
          }
        }
      }
      .sortByKey(ascending = true)
      .sortBy(_._2, ascending = false)
      .zipWithIndex
      .filter(x => x._2 < 5)
      .map(x => x._1)
      .join(movie_mappings)
      .sortByKey(ascending = true)
      .sortBy(x => x._2._1, ascending = false)
      .map { case (i, (rating, movie_name)) =>
        List(i, movie_name, rating)
      }

    return top_preds.collect().toList
  }

  println(top_films_imdb(30))
  println(top_films_imdb(300))

  ///////////////////////////////////////////////////////////////////

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
          "Q3.2.5" -> Map(
            "Top5WithK=30" -> top_movies_k30,
            "Top5WithK=300" -> top_movies_k300
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
