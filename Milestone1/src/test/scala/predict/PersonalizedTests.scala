package test.predict

import org.scalatest._
import funsuite._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._
import tests.shared.helpers._
import ujson._

class PersonalizedTests extends AnyFunSuite with BeforeAndAfterAll {

   val separator = "\t"
   var spark : org.apache.spark.sql.SparkSession = _

   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   var train2 : Array[shared.predictions.Rating] = null
   var test2 : Array[shared.predictions.Rating] = null

   override def beforeAll : Unit = {
       Logger.getLogger("org").setLevel(Level.OFF)
       Logger.getLogger("akka").setLevel(Level.OFF)
       spark = SparkSession.builder()
           .master("local[1]")
           .getOrCreate()
       spark.sparkContext.setLogLevel("ERROR")
       // For these questions, train and test are collected in a scala Array
       // to not depend on Spark
       train2 = load(spark, train2Path, separator).collect()
       test2 = load(spark, test2Path, separator).collect()
   }

   // All the functions definitions for the tests below (and the tests in other suites) 
   // should be in a single library, 'src/main/scala/shared/predictions.scala'.

   // Provide tests to show how to call your code to do the following tasks.
   // Ensure you use the same function calls to produce the JSON outputs in
   // src/main/scala/predict/Baseline.scala.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
   
   test("Test uniform unary similarities") { 

     // Compute personalized prediction for user 1 on item 1
     var t1 = timingInMs(() => predictorSimilarityUniform(train2)(1,1))
     println("Uniform: Prediction of user 1 on item 1", t1)
     assert(within(0.0, 0.0, 0.0001))

     // MAE 
     var t2 = timingInMs(() => computeMae(train2,test2,predictorSimilarityUniform))
     println("Uniform: MAE", t2)
     assert(within(0.0, 0.0, 0.0001))
   } 


   test("Test ajusted cosine similarity") { 

     // Similarity between user 1 and user 2
     var t3 = timingInMs(() => cosineSimilarity(train2)(1,2))
     println("Cosine similarity between user 1 and user 2", t3)
     assert(within(0.0, 0.0, 0.0001))

     // Compute personalized prediction for user 1 on item 1
     var t4 = timingInMs(() => predictorSimilarityCosine(train2)(1,1))
     println("Cosine: Prediction of user 1 on item 1", t4)
     assert(within(0.0, 0.0, 0.0001))

     // MAE 
     var t5 = timingInMs(() => computeMae(train2,test2,predictorSimilarityCosine))
     println("Cosine: MAE", t5)
     assert(within(0.0, 0.0, 0.0001))
   }


   test("Test jaccard similarity") { 

     // Similarity between user 1 and user 2
     var t6 = timingInMs(() => jaccardSimilarity(train2)(1,2))
     println("Jaccard similarity between user 1 and user 2", t6)
     assert(within(0.0, 0.0, 0.0001))

     // Compute personalized prediction for user 1 on item 1
     var t7 = timingInMs(() => predictorSimilarityJaccard(train2)(1,1))
     println("Jaccard: Prediction of user 1 on item 1", t7)
     assert(within(0.0, 0.0, 0.0001))

     // MAE 
     var t8 = timingInMs(() => computeMae(train2,test2,predictorSimilarityJaccard))
     println("Jaccard: MAE", t8)
     assert(within(0.0, 0.0, 0.0001))
   }
   
}
