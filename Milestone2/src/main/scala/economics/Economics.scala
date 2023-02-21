import org.rogach.scallop._
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import ujson._

package economics {

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) : Unit = {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

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

        val answers = ujson.Obj(
          "E.1" -> ujson.Obj(
            "MinRentingDays" -> ujson.Num(ceil(38600.0/20.40)) // Datatype of answer: Double
          ),
          "E.2" -> ujson.Obj(
            "ContainerDailyCost" -> ujson.Num((1.14e-6 + 4*8*1.6e-7) * 3600 * 24),
            "4RPisDailyCostIdle" -> ujson.Num(4.0*(24.0*3.0/1000.0)*0.25),
            "4RPisDailyCostComputing" -> ujson.Num(4.0*(24.0*4.0/1000.0)*0.25),
            "MinRentingDaysIdleRPiPower" -> ujson.Num(ceil((4.0*108.48)/(0.540864-0.072))),
            "MinRentingDaysComputingRPiPower" -> ujson.Num(ceil((4.0*108.48)/(0.540864-0.096))) 
          ),
          "E.3" -> ujson.Obj(
            "NbRPisEqBuyingICCM7" -> ujson.Num(floor(38600/108.48)),
            "RatioRAMRPisVsICCM7" -> ujson.Num((355.0*8.0)/(24.0*64.0)),
            "RatioComputeRPisVsICCM7" -> ujson.Num((355.0)/(2.0*2.6*14.0))
          )
        )

        val json = write(answers, 4)
        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}

}
