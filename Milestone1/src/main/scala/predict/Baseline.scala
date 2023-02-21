package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object Baseline extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  // For these questions, data is collected in a scala Array 
  // to not depend on Spark
  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator()).collect()
  

  val measurements_GlobalAvg = (1 to conf.num_measurements()).map(x => timingInMs(() => {
  computeMae(train,test,predictorGlobalAverage)
  }))
  val timings_GlobalAvg = measurements_GlobalAvg.map(t => t._2) // Retrieve the timing measurements

  val measurements_UserAvg = (1 to conf.num_measurements()).map(x => timingInMs(() => {
  computeMae(train,test,predictorUserAverage)
  }))
  val timings_UserAvg = measurements_UserAvg.map(t => t._2) // Retrieve the timing measurements

  val measurements_ItemAvg = (1 to conf.num_measurements()).map(x => timingInMs(() => {
  computeMae(train,test,predictorItemAverage)
  }))
  val timings_ItemAvg = measurements_ItemAvg.map(t => t._2) // Retrieve the timing measurements

  val measurements_Baseline = (1 to conf.num_measurements()).map(x => timingInMs(() => {
  computeMae(train,test,predictorBaseline)
  }))
  val timings_Baseline = measurements_Baseline.map(t => t._2) // Retrieve the timing measurements

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
      var answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "B.1" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Num(mean_rating(train)), // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(compute_user_i_average(train, 1)),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(compute_item_i_average(train, 1)),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(compute_item_i_average_deviation(train, 1)), // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(predictorBaseline(train)(1,1)) // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> ujson.Num(computeMae(train,test,predictorGlobalAverage)), // Datatype of answer: Double
          "2.UserAvgMAE" -> ujson.Num(computeMae(train,test,predictorUserAverage)),  // Datatype of answer: Double
          "3.ItemAvgMAE" -> ujson.Num(computeMae(train,test,predictorItemAverage)),   // Datatype of answer: Double
          "4.BaselineMAE" -> ujson.Num(computeMae(train,test,predictorBaseline))   // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings_GlobalAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings_GlobalAvg)) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings_UserAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings_UserAvg)) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings_ItemAvg)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings_ItemAvg)) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings_Baseline)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings_Baseline)) // Datatype of answer: Double
          )
        )
      )

      val json = ujson.write(answers, 4)
      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json.toString, jsonFile)
    }
  }

  println("")
  spark.close()
}
