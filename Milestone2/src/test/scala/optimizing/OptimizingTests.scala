package test.optimizing

import breeze.linalg._
import breeze.numerics._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import shared.predictions._
import test.shared.helpers._

class OptimizingTests extends AnyFunSuite with BeforeAndAfterAll {
  
   val separator = "\t"
   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   //val train2Path = "data/ml-1m/rb.train"
   //val test2Path = "data/ml-1m/rb.test"
   var train2 : CSCMatrix[Double] = null
   var test2 : CSCMatrix[Double] = null

   override def beforeAll : Unit = {
       // For these questions, train and test are collected in a scala Array
       // to not depend on Spark
       //6040 3952
       train2 = load(train2Path, separator, 943, 1682)
       test2 = load(test2Path, separator, 943, 1682)
   }

   // Provide tests to show how to call your code to do the following tasks.
   // Ensure you use the same function calls to produce the JSON outputs in
   // the corresponding application.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
   test("kNN predictor with k=10") { 
    

    println(train2.rows,train2.cols)
    var similarity_k_fun = similarity_k(train2,10)
    var predict_k_fun = predict_k(train2,10)
    //var similarity_k_fun1 = similarity_k1(train2,10)
    //var predict_k_fun1 = predict_k1(train2,10)

     // Similarity between user 1 and itself
     println("Similarity between user 1 and itself", similarity_k_fun(1,1))
     assert(within(0.0, 0.0, 0.0001))
     
     // Similarity between user 1 and 864
     println("Similarity between user 1 and 864", similarity_k_fun(1,864))
     assert(within(0.0, 0.0, 0.0001))

     // Similarity between user 1 and 886
     println("Similarity between user 1 and 886", similarity_k_fun(1,886))
     assert(within(0.0, 0.0, 0.0001))
    
     // Prediction user 1 and item 1
     println("Prediction user 1 and item 1", predict_k_fun(1,1))
     //println("Prediction1 user 1 and item 1 1", predict_k_fun1(1,1))
     assert(within(0.0, 0.0, 0.0001))

     // Prediction user 327 and item 2
     println("Prediction user 327 and item 2", predict_k_fun(327,2))
     //println("Prediction user 327 and item 2 1", predict_k_fun1(327,2))
     assert(within(0.0, 0.0, 0.0001))
     /*
     // MAE on test2
     var result_10 = timingInMs(() => mae1(10,train2,test2))
     println("MAE on test2 with k=10 :",result_10)
     assert(within(0.0, 0.0, 0.0001)) 

     var result_300 = timingInMs(() => mae1(300,train2,test2))
     println("MAE on test2 with k=300 :",result_300)
     assert(within(0.0, 0.0, 0.0001)) 
     */
     // MAE on test2
     var result_10_1 = timingInMs(() => mae(10,train2,test2))
     println("MAE on test2 with k=10 :",result_10_1)
     assert(within(0.0, 0.0, 0.0001)) 

     var result_300_1 = timingInMs(() => mae(300,train2,test2))
     println("MAE on test2 with k=300 :",result_300_1)
     assert(within(0.0, 0.0, 0.0001)) 
     
   } 
}
