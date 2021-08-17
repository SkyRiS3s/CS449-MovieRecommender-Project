import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)
    //----------------------------------------------------------------
    //1
    val n_days_1 = 35000.0/20.4
    val n_years_1 = n_days_1/365.0

    //2
    val price_ram_day_containers_2 = 24.0*64.0*0.012
    val price_CPU_ram_day_containers_2 = 2.0*14.0*0.088
    val price_daily_containers_2 = price_ram_day_containers_2 + price_CPU_ram_day_containers_2
    val ratio_2 = 20.40 / price_CPU_ram_day_containers_2
    val is_cheap: Boolean = (price_daily_containers_2 < 20.40) && scala.math.abs(price_daily_containers_2-20.40)/((20.40+price_daily_containers_2)/2) > 0.2

    //3
    val v_cpu_price_day_3 = 0.088
    val GB_price_per_day_3 = 0.012*8
    val daily_cost_3 = v_cpu_price_day_3 +GB_price_per_day_3
    val ratio_max_3 = 0.054 / daily_cost_3
    val ratio_min_3 = 0.0108 / daily_cost_3
    val is_cheaper_3 : Boolean = (daily_cost_3 < 0.0108) && (scala.math.abs(daily_cost_3 - 0.054)/((daily_cost_3+0.054)/2) > 0.2)

    //4
    val cost_buying_4rpi_4 =  94.83
    val cost_min_4rpi_per_day_4 = 0.0108
    val cost_container_per_day = 0.012+0.088+5.68
    val min_days_4 = cost_buying_4rpi_4 / (cost_container_per_day - cost_min_4rpi_per_day_4)
    val c_max = 0.054
    val max_days_4 = cost_buying_4rpi_4 /(cost_container_per_day - c_max)


    //5
    val nb_rpi_5: Double = scala.math.floor(35000/94.83)
    val ratio_ram_5 = (nb_rpi_5*8)/(24.0*64.0)
    val ratio_throughput = (nb_rpi_5)/(4*2*14)



    //6
    val user_size_6 = (100*8+200*32)/ 1e9
    val user_perGB_6 = 1.0/user_size_6
    val user_perRpi_6 = 8.0/2 * user_perGB_6
    val user_perICCM7_6 = 24*64/2 * user_perGB_6











    //----------------------------------------------------------------
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
            "Q5.1.1" -> Map(
              "MinDaysOfRentingICC.M7" -> n_days_1, // Datatype of answer: Double
              "MinYearsOfRentingICC.M7" -> n_years_1 // Datatype of answer: Double
            ),
            "Q5.1.2" -> Map(
              "DailyCostICContainer_Eq_ICC.M7_RAM_Throughput" -> price_daily_containers_2, // Datatype of answer: Double
              "RatioICC.M7_over_Container" -> ratio_2, // Datatype of answer: Double
              "ContainerCheaperThanICC.M7" -> is_cheap // Datatype of answer: Boolean
            ),
            "Q5.1.3" -> Map(
              "DailyCostICContainer_Eq_4RPi4_Throughput" -> daily_cost_3, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MaxPower" -> ratio_max_3, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MinPower" -> ratio_min_3, // Datatype of answer: Double
              "ContainerCheaperThan4RPi" -> is_cheaper_3 // Datatype of answer: Boolean
            ),
            "Q5.1.4" -> Map(
              "MinDaysRentingContainerToPay4RPis_MinPower" -> min_days_4, // Datatype of answer: Double
              "MinDaysRentingContainerToPay4RPis_MaxPower" -> max_days_4 // Datatype of answer: Double
            ),
            "Q5.1.5" -> Map(
              "NbRPisForSamePriceAsICC.M7" -> nb_rpi_5, // Datatype of answer: Double
              "RatioTotalThroughputRPis_over_ThroughputICC.M7" -> ratio_throughput, // Datatype of answer: Double
              "RatioTotalRAMRPis_over_RAMICC.M7" -> ratio_ram_5 // Datatype of answer: Double
            ),
            "Q5.1.6" ->  Map(
              "NbUserPerGB" -> user_perGB_6, // Datatype of answer: Double
              "NbUserPerRPi" -> user_perRpi_6, // Datatype of answer: Double
              "NbUserPerICC.M7" -> user_perICCM7_6// Datatype of answer: Double
            )
            // Answer the Question 5.1.7 exclusively on the report.
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
