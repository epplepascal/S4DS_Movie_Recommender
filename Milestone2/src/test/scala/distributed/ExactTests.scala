package test.distributed

import breeze.linalg._
import breeze.numerics._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import shared.predictions._
import test.shared.helpers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

class ExactTests extends AnyFunSuite with BeforeAndAfterAll {
  
   val separator = "\t"
   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   var train2 : CSCMatrix[Double] = null
   var test2 : CSCMatrix[Double] = null
   var sc : SparkContext = null

   override def beforeAll : Unit = {
     train2 = load(train2Path, separator, 943, 1682)
     test2 = load(test2Path, separator, 943, 1682)

     val spark = SparkSession.builder().master("local[2]").getOrCreate();
     spark.sparkContext.setLogLevel("ERROR")
     sc = spark.sparkContext
   }

   // Provide tests to show how to call your code to do the following tasks.
   // Ensure you use the same function calls to produce the JSON outputs in
   // the corresponding application.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
   test("kNN predictor with k=10") { 

     // Similarity between user 1 and itself
     println("Similarity between user 1 and 1",similarity_k_exact(train2,10,sc)(1,1))
     assert(within(0.0, 0.0, 0.0001))
 
     // Similarity between user 1 and 864
     println("Similarity between user 1 and 864",similarity_k_exact(train2,10,sc)(1,864))
     assert(within(0.0, 0.0, 0.0001))

     // Similarity between user 1 and 886
     println("Similarity between user 1 and 886",similarity_k_exact(train2,10,sc)(1,886))
     assert(within(0.0, 0.0, 0.0001))

     // Prediction user 1 and item 1
     println("Prediction user 1 and item 1",predict_k_exact(train2,10,sc)(1,1))
     assert(within(0.0, 0.0, 0.0001))

     // Prediction user 327 and item 2
     println("Prediction user 327 and item 2",predict_k_exact(train2,10,sc)(327,2))
     assert(within(0.0, 0.0, 0.0001))

     // MAE on test2
     var result_10 = timingInMs(() => mae_exact(train2,test2,10,sc))
     println("MAE on test2 with k=10 :",result_10)
     assert(within(0.0, 0.0, 0.0001)) 

     var result_300 = timingInMs(() => mae_exact(train2,test2,300,sc))
     println("MAE on test2 with k=300 :",result_300)
     //assert(within(0.0, 0.0, 0.0001)) 
   } 
}
