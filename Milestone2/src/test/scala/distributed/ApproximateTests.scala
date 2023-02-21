package test.distributed

import breeze.linalg._
import breeze.numerics._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import shared.predictions._
import test.shared.helpers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

class ApproximateTests extends AnyFunSuite with BeforeAndAfterAll {
  
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
   test("Approximate kNN predictor with 10 partitions and replication of 2") { 
    var partitionedUsers : Seq[Set[Int]] = partitionUsers(
      943, 
      10, 
      2
    ) //nb users, nbPartition, replication

     // Similarity between user 1 and itself
    
    /*
     var sim = approximateKNNSimilarity(train2,partitionedUsers, 10, sc)
     println(sim(1,1))
     println(sim(1,864))
     println(sim(1,344))
     println(sim(1,16))
     println(sim(1,334))
     println(sim(1,2))
     */
     
    
     var k = 10

     var mymae = mae_approximate(train2,test2,k,partitionedUsers,sc)

     println(mymae)
    

    /*
     println("Similarity between user 1 and 1",approximateKNNSimilarity(train2,partitionedUsers, 10, sc)(1,1))
     assert(within(0.0, 0.0, 0.0001))
 
     // Similarity between user 1 and 864
     println("Similarity between user 1 and 864",approximateKNNSimilarity(train2,partitionedUsers, 10, sc)(1,864))
     assert(within(0.0, 0.0, 0.0001))

     // Similarity between user 1 and 344
     println("Similarity between user 1 and 344",approximateKNNSimilarity(train2,partitionedUsers, 10, sc)(1,344))

     assert(within(0.0, 0.0, 0.0001))

     // Similarity between user 1 and 16
     println("Similarity between user 1 and 16",approximateKNNSimilarity(train2,partitionedUsers, 10, sc)(1,16))
     assert(within(0.0, 0.0, 0.0001))

     // Similarity between user 1 and 334
     println("Similarity between user 1 and 334",approximateKNNSimilarity(train2,partitionedUsers, 10, sc)(1,334))
     assert(within(0.0, 0.0, 0.0001))

     // Similarity between user 1 and 2
     println("Similarity between user 1 and 2",approximateKNNSimilarity(train2,partitionedUsers, 10, sc)(1,2))
     assert(within(0.0, 0.0, 0.0001))

     // MAE on test
     assert(within(0.0, 0.0, 0.0001))

     */
     
   } 

  /*

  test("My function test"){
       var mylist = (1 to 943).toList //indc 
       var myset = mylist.toSet
       //var (user_avg, global_avg, normalized_deviation, preprocessed_train, nbr_rows, nbr_cols) = preprocess_data_exact(train2)
       
       var similarities = similarity_test_b(train2,10,myset)

       println(similarities(1,864))
       println(similarities(2,3))
       println(similarities(1,1))
       println(similarities(3,4))

    }

    test("with an RDD partition") {
      var partitionedUsers : Seq[Set[Int]] = partitionUsers(
      943, 
      10, 
      2 
    )

    var mylist = (1 to 943).toList //indc 
    var myset : Seq[Set[Int]] = Seq(mylist.toSet)
  

    var sim = similarity_test_final(train2,10,myset,sc)
    println(sim(1,1))
    println(sim(1,864))
    println(sim(1,344))
    println(sim(1,16))
    }

  test("partition test") {
        var partitionedUsers : Seq[Set[Int]] = partitionUsers(
      943, 
      10, 
      10 
    )
    println(partitionedUsers.filter(p => p.contains(1)).count(z => true))
    println(partitionedUsers.filter(p => p.contains(2)).count(z => true))
    println(partitionedUsers.filter(p => p.contains(3)).count(z => true))
    println(partitionedUsers.filter(p => p.contains(4)).count(z => true))

  }
  */

  /*
  test("update works?"){

    val builder = new CSCMatrix.Builder[Double](3,3) 
    builder.add(0,0,1.0)
    builder.add(0,1,1.0)
    builder.add(0,2,1.0)
    builder.add(1,0,1.0)
    builder.add(1,1,1.0)
    builder.add(1,2,1.0)
    builder.add(2,0,1.0)
    builder.add(2,1,1.0)
    builder.add(2,2,1.0)

    val mymatrix = builder.result()
    print(mymatrix)
    var myset1 = Set(1)
    var myset2 = Set(2)
    var partition = Seq(myset1, myset2)
    partition.map(p => extract_columns(mymatrix,p)).foreach(println)
   
  } 

  */

  /*

  test("node test") {
     var partitionedUsers : Seq[Set[Int]] = partitionUsers(
      943, 
      10, 
      2 
    )

      var fullData : Seq[Set[Int]] = partitionUsers(
      943, 
      1, 
      1
    )
   
    //mypart.map(part => Map(part -> extract_columns(train2,part))).map{case (part,mat) => nodeSimilarity(part,mat,10)}

    /*
    var mymap = partitionedUsers.map(part => {
      var mat = extract_columns(train2,part)
      nodeSimilarity(mat,part,10) 
    })
    */
    var train = sc.broadcast(train2)
    var mymap1 = sc.parallelize(partitionedUsers).map(part => {
      var mat = extract_columns(train.value,part)
      nodeSimilarity(mat,part,10) 
    })

    var collected : Array[Map[Int,List[(Int, Double)]]] = mymap1.collect()
    var array1 = collected(1)
    var array2 : Map[Int,List[(Int, Double)]] = collected(2)
    var array3 : Map[Int,List[(Int, Double)]] = collected(3)
    var array4 : Map[Int,List[(Int, Double)]] = collected(4)
    var array5 : Map[Int,List[(Int, Double)]] = collected(5)
    var array6 : Map[Int,List[(Int, Double)]] = collected(6)
    var array7 : Map[Int,List[(Int, Double)]] = collected(7)
    var array8 : Map[Int,List[(Int, Double)]] = collected(8)
    var array9 : Map[Int,List[(Int, Double)]] = collected(9)
    var array10 : Map[Int,List[(Int, Double)]] = collected(0)

    println("Every generated array")
    println(array1.get(1))
    println(array2.get(1))
    println(array3.get(1))
    println(array4.get(1))
    println(array5.get(1))
    println(array6.get(1))
    println(array7.get(1))
    println(array8.get(1))
    println(array9.get(1))
    println(array10.get(1))

    println("Combined array")

    //println((array1 ++ array2 ++ array2 ++ array3 ++ array4 ++ array5 ++ array6 ++ array7 ++ array8 ++ array9 ++ array10).get(1))


    var finalmap : Map[Int,List[(Int, Double)]] = collected.reduce((map1,map2) => map1 ++ map2.map{ case (k,v) => k -> (v ++ map1.getOrElse(k,Nil))})
    //var testmap = finalmap.groupBy(_._1).map{case (k,v) => k -> v}
    var testmap = finalmap.map{case (k,v) => k -> v.toSet.toList} //works
    var mysizes = testmap.map{case (key,v) => key -> v.size} //works
    
    print(testmap.get(1))

    println("sizes before")
    println(mysizes)
    var mapfinale = testmap.map{case (key,v) => key -> findKLargestElements(10)(v)}
    println("sizes after")
    mysizes = mapfinale.map{case (key,v) => key -> v.size}
    println(mysizes)

    //faut construire la matrice maintenant, ou alors on peut aussi garder la map ? 
    //faire prediction mais ça devrait aller vu que tu as déjà calculé toutes les similarités
    

  }
*/

/*

test("spark test") {
    var partitionedUsers : Seq[Set[Int]] = partitionUsers(
      943, 
      10, 
      8 
    )
    var mymap =  approximateKNNSimilarity(train2,partitionedUsers, 300, sc)
}
*/

}

