package test.recommend

import org.scalatest._
import funsuite._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._
import tests.shared.helpers._
import ujson._

class RecommenderTests extends AnyFunSuite with BeforeAndAfterAll {

   val separator = "\t"
   var spark : org.apache.spark.sql.SparkSession = _

   val dataPath = "data/ml-100k/u.data"
   val personalPath = "data/personal.csv"
   var data : Array[shared.predictions.Rating] = null
   var personal : Array[shared.predictions.Rating] = null
   var train : Array[shared.predictions.Rating] = null
   var predictor : (Int, Int) => Double = null
   var name_to_id :Array[(Int,String)] = null


   override def beforeAll : Unit = {
     Logger.getLogger("org").setLevel(Level.OFF)
     Logger.getLogger("akka").setLevel(Level.OFF)
     spark = SparkSession.builder()
         .master("local[1]")
         .getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
    
     data = load(spark, dataPath, separator).collect()

     println("Loading personal data from: " + personalPath) 
     val personalFile = spark.sparkContext.textFile(personalPath)
     personal = personalFile.map(l => {
         val cols = l.split(",").map(_.trim)
         if (cols(0) == "id") 
           Rating(944,0,0.0)
         else 
           if (cols.length < 3) 
             Rating(944, cols(0).toInt, 0.0)
           else
             Rating(944, cols(0).toInt, cols(2).toDouble)
     }).filter(r => r.rating != 0).collect()

     name_to_id = personalFile.map(l => {
         val cols = l.split(",").map(_.trim)
         if (cols(0) == "id") 
            (0, cols(1))
          else 
             (cols(0).toInt, cols(1))
     }).collect()

    
   }

   // All the functions definitions for the tests below (and the tests in other suites) 
   // should be in a single library, 'src/main/scala/shared/predictions.scala'.
  
 
   test("Prediction for user 1 of item 1") {
     //cosine and k = 300
     var result_1 = timingInMs(() => predictorSimilarity_k(data.union(personal),300)(1,1))
     println("Prediction for user 1 of item 1 with k=300 and cosine ",result_1)
     assert(within(result_1._1,  4.132180229734752, 0.0001))
   }

   test("Top 3 recommendations for user 944") {

     // first argument : user (u=944)
     // second argument : top n predictions (n=3)
     var result_2 = best_i_item_for_user_u(data.union(personal),predictorSimilarity_k,300)(944,3)                      
     println("Return of prediction ",result_2)


   }
   

}
