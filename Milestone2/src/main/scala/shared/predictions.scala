package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

package object predictions
{
  // ------------------------ For template
  case class Rating(user: Int, item: Int, rating: Double)

  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

  object MinOrder extends Ordering[(Int,Double)] {
         def compare(x:(Int,Double), y:(Int,Double)) = y._2 compare x._2
       }


  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0

  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else { 
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble) 
    }
  }

  def load(path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = Source.fromFile(path)
    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies) 
    for (line <- file.getLines) {
      val cols = line.split(sep).map(_.trim)
      toInt(cols(0)) match {
        case Some(_) => builder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
        case None => None
      }
    }
    file.close
    builder.result()
  }

  def loadSpark(sc : org.apache.spark.SparkContext,  path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = sc.textFile(path)
    val ratings = file
      .map(l => {
        val cols = l.split(sep).map(_.trim)
        toInt(cols(0)) match {
          case Some(_) => Some(((cols(0).toInt-1, cols(1).toInt-1), cols(2).toDouble))
          case None => None
        }
      })
      .filter({ case Some(_) => true
                 case None => false })
      .map({ case Some(x) => x
             case None => ((-1, -1), -1) }).collect()
   
    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies)
    for ((k,v) <- ratings) {
      v match {
        case d: Double => {
          val u = k._1
          val i = k._2
          builder.add(u, i, d)
        }
      }
    }
    return builder.result
  }

  def partitionUsers (nbUsers : Int, nbPartitions : Int, replication : Int) : Seq[Set[Int]] = {
    val r = new scala.util.Random(1337)
    val bins : Map[Int, collection.mutable.ListBuffer[Int]] = (0 to (nbPartitions-1))
       .map(p => (p -> collection.mutable.ListBuffer[Int]())).toMap
    (0 to (nbUsers-1)).foreach(u => {
      val assignedBins = r.shuffle(0 to (nbPartitions-1)).take(replication)
      for (b <- assignedBins) {
        bins(b) += u
      }
    })
    bins.values.toSeq.map(_.toSet)
  }

  def scale(x : Double, r_avg_u : Double): Double = x compare r_avg_u match {
    case 1 => 5.0 - r_avg_u
    case -1 => r_avg_u - 1.0
    case 0 => 1.0
  } 

//////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////// Optimizing ////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
 def preprocess_data1(train: CSCMatrix[Double], k: Int): (DenseVector[Double], Double, CSCMatrix[Double], CSCMatrix[Double], Int, Int) = {
    
    val nbr_rows = train.rows
    val nbr_cols = train.cols

    // sum of ratings per user
    val numerator = (train * DenseVector.ones[Double](nbr_cols))
    // number of elements per user (replace 0.0 by 1.0)
    val denominator = ((train.map(elem => if (elem != 0.0) 1.0 else 0.0)) * DenseVector.ones[Double](nbr_cols)).map(elem => if (elem == 0.0) 1.0 else elem)

    // average per user
    val user_avg = {
      // Elementwise division
      numerator /:/ denominator}
    
    // global average 
    val global_avg = sum(numerator) / sum(denominator)
    
    val normalized_deviation = {
      val normalized_deviation_CSC = new CSCMatrix.Builder[Double](rows=nbr_rows, cols=nbr_cols)
      for ((k,v) <- train.activeIterator) {
        val row = k._1
        val col = k._2
        normalized_deviation_CSC.add(row, col, (v - user_avg(row))/scale(v, user_avg(row)))
      }
      normalized_deviation_CSC.result()
    }

    val preprocessed_denum = (normalized_deviation *:* normalized_deviation * DenseVector.ones[Double](nbr_cols)).map(i => math.sqrt(i))

    val preprocessed  = normalized_deviation /:/ (preprocessed_denum * (DenseVector.ones[Double](nbr_cols).t))
  
    val similarity_sparse_builder = new CSCMatrix.Builder[Double](rows=nbr_rows, cols=nbr_cols) 

    (0 to nbr_rows-1).map(user => {
      val similarity_per_user = preprocessed * preprocessed(user, 0 to nbr_cols-1).t
      similarity_per_user(user) = 0.0
      for (user_temp <- argtopk(similarity_per_user, k)){
        similarity_sparse_builder.add(user, user_temp, similarity_per_user(user_temp))
      } 
    })
    val similarity_sparse = similarity_sparse_builder.result()

    // refaire code avec un top k 
    // création de matrix CSC et remplir pour chaque ligne
    // changer type dans function preprocess_data
    // changer implementation dans les 3 autres fonctions -> itérer sur tout le slice 
    // si ca prend trop de temps, conserver en memoire les topk indices et itérer dedans
    (user_avg, global_avg, normalized_deviation, similarity_sparse, nbr_rows, nbr_cols)

  }
  
  
  def predict_k1(train: CSCMatrix[Double], k: Int) : ((Int,Int) => Double) = {
    var (user_avg, global_avg, normalized_deviation, similarity_sparse, nbr_rows, nbr_cols) = preprocess_data1(train,k)

    // u : user id
    // i : item id 
    (u:Int, i:Int) => {
      // element (user or movie) out of the matrix boundaries
      if (u>nbr_rows || i>nbr_cols)
        global_avg
      // rating already exists 
      else if (train(u-1,i-1) != 0.0)
        train(u-1,i-1)
      else {
        var u_topk = similarity_sparse(u-1, 0 to similarity_sparse.rows-1).t
        val numerator = u_topk.toArray.zipWithIndex.map{case (x,index) => if (x != 0.0) normalized_deviation(index, i-1)*x else 0.0}.sum
        val denominator = u_topk.toArray.zipWithIndex.map{case (x,index) => if (train(index, i-1) != 0.0) x.abs else 0.0}.sum
        var avg_user = user_avg(u-1)

        if (denominator == 0.0)
          avg_user
        else {
          var dev_user = numerator/denominator 
          avg_user + dev_user * scale((avg_user + dev_user), avg_user)
        }
      }
    } 
  }
  
  def similarity_k1(train: CSCMatrix[Double], k: Int) : ((Int,Int) => Double) = {
    var (_, _, _, similarity_sparse, nbr_rows, nbr_cols) = preprocess_data1(train,k)

    // u : user id
    // v : user id 
    (u:Int, v:Int) => {
      if (u<=nbr_rows && v<=nbr_cols)
        similarity_sparse(u-1, v-1)
      else 
        0.0
      } 
  }
  
  // MAE
  def mae1(k: Int, train: CSCMatrix[Double], test: CSCMatrix[Double]) = {
    var (user_avg, global_avg, normalized_deviation, similarity_sparse, nbr_rows, nbr_cols) = preprocess_data1(train,k)
    
    val prediction = {
      val prediction_CSC = new CSCMatrix.Builder[Double](rows=test.rows, cols=test.cols)
      for ((key,v) <- test.activeIterator)
      { 
        val user = key._1
        val item = key._2
        if (user>nbr_rows || item>nbr_cols)
          // element (user or movie) out of the train matrix dimensions
          prediction_CSC.add(user, item, global_avg)
        else {
          // garder code de raph mais calculer les k valeurs plus grande avant!!
          var u_topk = similarity_sparse(user, 0 to similarity_sparse.rows-1).t.toArray.zipWithIndex
          val numerator = u_topk.map{case (x,index) => if (x != 0.0) normalized_deviation(index, item)*x else 0.0}.sum
          val denominator = u_topk.map{case (x,index) => if (train(index, item) != 0.0) x.abs else 0.0}.sum

          var avg_user = user_avg(user)
          if (denominator == 0.0)
            prediction_CSC.add(user, item, avg_user)
          else {
            var dev_user = numerator/denominator
            prediction_CSC.add(user, item, avg_user + dev_user * scale((avg_user + dev_user), avg_user))
          }
        }
      }
      prediction_CSC.result()
    }

    var sum_error = 0.0
    for ((k,v) <- test.activeIterator){
      sum_error = sum_error + (v - prediction(k)).abs
    }
    sum_error/(test.activeSize)
  }
  */
  
  def preprocess_data(train: CSCMatrix[Double]): (DenseVector[Double], Double, CSCMatrix[Double], IndexedSeq[Vector[Double]], Int, Int) = {
    
    val nbr_rows = train.rows
    val nbr_cols = train.cols

    // sum of ratings per user
    val numerator = (train * DenseVector.ones[Double](nbr_cols))
    // number of elements per user (replace 0.0 by 1.0)
    val denominator = ((train.map(elem => if (elem != 0.0) 1.0 else 0.0)) * DenseVector.ones[Double](nbr_cols)).map(elem => if (elem == 0.0) 1.0 else elem)

    // average per user
    val user_avg = {
      // Elementwise division
      numerator /:/ denominator}
    
    // global average 
    val global_avg = sum(numerator) / sum(denominator)
    
    val normalized_deviation = {
      val normalized_deviation_CSC = new CSCMatrix.Builder[Double](rows=nbr_rows, cols=nbr_cols)
      for ((k,v) <- train.activeIterator) {
        val row = k._1
        val col = k._2
        normalized_deviation_CSC.add(row, col, (v - user_avg(row))/scale(v, user_avg(row)))
      }
      normalized_deviation_CSC.result()
    }

    val preprocessed_denum = (normalized_deviation *:* normalized_deviation * DenseVector.ones[Double](nbr_cols)).map(i => if (i != 0.0) math.sqrt(i) else 1.0)

    val preprocessed  = normalized_deviation /:/ (preprocessed_denum * (DenseVector.ones[Double](nbr_cols).t))
    
    val similarity = (0 to nbr_rows-1).map(user => {
      val similarity_per_user = preprocessed * preprocessed(user, 0 to nbr_cols-1).t
      similarity_per_user(user) = 0.0
      similarity_per_user
    }) 
    //check ici en haut tu en as besoin

    // refaire code avec un top k 
    // création de matrix CSC et remplir pour chaque ligne
    // changer type dans function preprocess_data
    // changer implementation dans les 3 autres fonctions -> itérer sur tout le slice 
    // si ca prend trop de temps, conserver en memoire les topk indices et itérer dedans
    (user_avg, global_avg, normalized_deviation, similarity, nbr_rows, nbr_cols)

  }
  

  def predict_k(train: CSCMatrix[Double], k: Int) : ((Int,Int) => Double) = {
    var (user_avg, global_avg, normalized_deviation, similarity, nbr_rows, nbr_cols) = preprocess_data(train)

    // u : user id
    // i : item id 
    (u:Int, i:Int) => {
      // element (user or movie) out of the matrix boundaries
      if (u>nbr_rows || i>nbr_cols)
        global_avg
      // rating already exists 
      else if (train(u-1,i-1) != 0.0)
        train(u-1,i-1)
      else {
        val user_similarity = similarity(u-1)
        var numerator = 0.0
        var denominator = 0.0
        for(user_topk <- argtopk(user_similarity,k)){ 
          if (user_similarity(user_topk) != 0.0 && train(user_topk, i-1) != 0.0){
            numerator += (user_similarity(user_topk) * normalized_deviation(user_topk, i-1))
            denominator += (user_similarity(user_topk).abs)}
        }

        var avg_user = user_avg(u-1)
        if (denominator == 0.0)
          avg_user
        else {
          var dev_user = numerator/denominator 
          avg_user + dev_user * scale((avg_user + dev_user), avg_user)
        }
      }
    } 
  }

  //check ici en bas tu en auras besoin

  def similarity_k(train: CSCMatrix[Double], k: Int) : ((Int,Int) => Double) = {
    var (_, _, _, similarity, nbr_rows, nbr_cols) = preprocess_data(train)

    val similarity_sparse = new CSCMatrix.Builder[Double](rows=nbr_rows, cols=nbr_cols) 
    for (row <- 0 to nbr_rows-1) {
      for (col <- argtopk(similarity(row), k)){
        similarity_sparse.add(row,col,similarity(row)(col))
      } 
    } 
    val similarity_sparse1 = similarity_sparse.result()

    // u : user id
    // v : user id 
    (u:Int, v:Int) => {
      if (u<=nbr_rows && v<=nbr_cols)
        similarity_sparse1(u-1, v-1)
      else 
        0.0
      } 
  }

  // MAE
  def mae(k: Int, train: CSCMatrix[Double], test: CSCMatrix[Double]) = {
    var (user_avg, global_avg, normalized_deviation, similarity, nbr_rows, nbr_cols) = preprocess_data(train)
    
    val prediction = {
      val prediction_CSC = new CSCMatrix.Builder[Double](rows=test.rows, cols=test.cols)
      for ((key,v) <- test.activeIterator)
      { 
        val user = key._1
        val item = key._2
        if (user>nbr_rows || item>nbr_cols)
          // element (user or movie) out of the train matrix dimensions
          prediction_CSC.add(user, item, global_avg)
        else {
          var numerator = 0.0
          var denominator = 0.0
          val user_similarity = similarity(user)
          for(user_topk <- argtopk(user_similarity,k)) 
          { 
            if (user_similarity(user_topk) != 0.0 && train(user_topk, item) != 0.0){
              numerator += (user_similarity(user_topk)*normalized_deviation(user_topk, item))
              denominator += (user_similarity(user_topk).abs)}
          }
          var avg_user = user_avg(user)
          if (denominator == 0.0)
            prediction_CSC.add(user, item, avg_user)
          else {
            var dev_user = numerator/denominator
            prediction_CSC.add(user, item, avg_user + dev_user * scale((avg_user + dev_user), avg_user))
          }
        }
      }
      prediction_CSC.result()
    }

    var sum_error = 0.0
    for ((k,v) <- test.activeIterator){
      sum_error = sum_error + (v - prediction(k)).abs
    }
    sum_error/(test.activeSize)
  }
  
//////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////// Exact /////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////

  def preprocess_data_exact(train: CSCMatrix[Double]): (DenseVector[Double], Double, CSCMatrix[Double], Matrix[Double], Int, Int) = {
    
    val nbr_rows = train.rows
    val nbr_cols = train.cols

    // sum of ratings per user
    val numerator = (train * DenseVector.ones[Double](nbr_cols))
    // number of elements per user (replace 0.0 by 1.0)
    val denominator = ((train.map(elem => if (elem != 0.0) 1.0 else 0.0)) * DenseVector.ones[Double](nbr_cols)).map(elem => if (elem == 0.0) 1.0 else elem)

    // average per user
    val user_avg = {
      // Elementwise division
      numerator /:/ denominator}
    
    // global average 
    val global_avg = sum(numerator) / sum(denominator)
    
    val normalized_deviation = {
      val normalized_deviation_CSC = new CSCMatrix.Builder[Double](rows=nbr_rows, cols=nbr_cols)
      for ((k,v) <- train.activeIterator) {
        val row = k._1
        val col = k._2
        normalized_deviation_CSC.add(row, col, (v - user_avg(row))/scale(v, user_avg(row)))
      }
      normalized_deviation_CSC.result()
    }

    val preprocessed_denum = (normalized_deviation *:* normalized_deviation * DenseVector.ones[Double](nbr_cols)).map(i => math.sqrt(i))

    val preprocessed  = normalized_deviation /:/ (preprocessed_denum * (DenseVector.ones[Double](nbr_cols).t))
    
    (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols)

  }
  
  def mae_exact(train: CSCMatrix[Double], test: CSCMatrix[Double], k: Int, sc: SparkContext) : Double = {

    var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)

    val br = sc.broadcast(preprocessed)

    def topk(u: Int) = {
    val similarity = {
      val s = ((br.value)*(br.value(u,0 to br.value.cols-1).t))
      s(u) = 0.0
      s
    } 

    (u, argtopk(similarity,k).map(v => (v,similarity(v))))
    }
    
    val topks = sc.parallelize(0 to train.rows-1).map(u => (topk(u))).collect()

    // Predictions
    val train_broadcast = sc.broadcast(train)
    val normalized_deviation_broadcast = sc.broadcast(normalized_deviation)

    def deviation_(u: Int, i: Int) = {
      val train_broadcasted = train_broadcast.value
      val normalized_deviation_broadcasted = normalized_deviation_broadcast.value
      val u_topk = topks(u)._2

      val denum = u_topk.map{case (v,s) => if (train_broadcasted(v,i) == 0.0 || s == 0.0) 0.0 else (math.abs(s))}.sum
      val num = u_topk.map{case (v,s) => if (train_broadcasted(v,i) == 0.0 || s == 0.0) 0.0 else (normalized_deviation_broadcasted(v,i)*s)}.sum

      {if (denum == 0.0) 0.0 else num/denum}
      }

    val deviations = sc.parallelize((for((k,v) <- test.activeIterator) yield k).toSeq).map{case (u,i) => ((u,i),deviation_(u,i))}

    val prediction = deviations.map{case ((u,i),dev) => ((u,i),{val mean = user_avg(u); mean+dev*scale(mean+dev,mean)})}

    val mae =(prediction.map{case ((u,i),v) =>{val s = math.abs(test(u,i)-v); if (s.isNaN) 0.0 else s}}.sum)/test.activeSize
    mae
  }

  def similarity_k_exact(train: CSCMatrix[Double], k: Int, sc: SparkContext) : ((Int,Int) => Double) = {
      var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)
      val br = sc.broadcast(preprocessed)

      def topk(u: Int) = {
        val similarity = {
          val s = ((br.value)*(br.value(u,0 to br.value.cols-1).t))
          s(u) = 0.0
          s
        } 
        (u,argtopk(similarity,k).map(v => (v,similarity(v))))
      }

    (u:Int, v:Int) => {
      topk(u-1)._2.toMap.getOrElse(v-1,0.0)
    }
  }

  def predict_k_exact(train: CSCMatrix[Double], k: Int, sc: SparkContext) : ((Int,Int) => Double) = {

    var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)

    val br = sc.broadcast(preprocessed)

    def topk(u: Int) = {
    val similarity = {
      val s = ((br.value)*(br.value(u,0 to br.value.cols-1).t))
      s(u) = 0.0
      s
    } 
    (u,argtopk(similarity,k).map(v => (v,similarity(v))))
    }
    
    val topks = sc.parallelize(0 to train.rows-1).map(u => (topk(u))).collect()

  // Predictions
    val train_broadcast = sc.broadcast(train)
    val normalized_deviation_broadcast = sc.broadcast(normalized_deviation)

    def deviation_(u: Int, i: Int) : Double = {
      val train_broadcasted = train_broadcast.value
      val normalized_deviation_broadcasted = normalized_deviation_broadcast.value
      val u_topk = topks(u)._2

      val denum = u_topk.map{case (v,s) => if (train_broadcasted(v,i) == 0.0 || s == 0.0) 0.0 else (math.abs(s))}.sum
      val num = u_topk.map{case (v,s) => if (train_broadcasted(v,i) == 0.0 || s == 0.0) 0.0 else (normalized_deviation_broadcasted(v,i)*s)}.sum

      {if (denum == 0.0) 0.0 else num/denum}
    }

    (u:Int, i:Int) => {
      // element (user or movie) out of the matrix boundaries
      if (u>nbr_rows || i>nbr_cols)
        global_avg
      // rating already exists 
      else if (train(u-1,i-1) != 0.0)
        train(u-1,i-1)
      else {
        /// 
        val dev = deviation_(u-1, i-1)
        val mean = user_avg(u-1) 
        mean+dev*scale(mean+dev,mean)
      }
    }
  } 
  
//////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////// Approximate ///////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////

  def test_fct(train: CSCMatrix[Double], k: Int, partition : Seq[Set[Int]], sc: SparkContext) : ((Int,Int) => Double) = {
      
      val myrdd = sc.parallelize(partition)
      myrdd.map(x => {
      var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)
      5.0
      }).foreach(x => println(x))


      println("je suis là")
      
    (u:Int, v:Int) => {
      5.0
    }
  }


  def similarity_k_approximate(train: CSCMatrix[Double], k: Int, partition : Seq[Set[Int]], sc: SparkContext) : ((Int,Int) => Double) = {
      var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)
      
      println("je suis la")
      val myrdd = sc.parallelize(partition)
      myrdd.map(x => {
      var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)
      preprocessed
      }).foreach(x => println(x))

      val br = sc.broadcast(preprocessed)

      def topk(u: Int) = {
        val similarity = {
          val s = ((br.value)*(br.value(u,0 to br.value.cols-1).t))
          s(u) = 0.0
          s
        } 
        (u,argtopk(similarity,k).map(v => (v,similarity(v))))
      }

    (u:Int, v:Int) => {
      topk(u-1)._2.toMap.getOrElse(v-1,0.0)
    }
  }

  def similarity_test_a(preprocessed_data: CSCMatrix[Double], k: Int, partition : Set[Int], sc: SparkContext) : ((Int,Int) => Double) = { 
  // déjà sur le noeud
  //on pourrait preprocess le train sur le master node et broadcast les valeurso obtenues
  //preprocess est une CSC Matrix 
    //var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)
    
    //we zero the elements that are not in the partition
    for ((k,v) <- preprocessed_data.activeIterator) {
        val user = k._1
        if (!partition.contains(user)) {preprocessed_data.update(k._1,k._2,0.0)}  
    }

    //var preprocessed_train = preprocessed_data.value

    def topk(u: Int) = {
        val similarity = {
          val s = ((preprocessed_data)*(preprocessed_data(u,0 to preprocessed_data.cols-1).t))
          s(u) = 0.0
          s
        } 
      (u,argtopk(similarity,k).map(v => (v,similarity(v))))
    }

    (u:Int, v:Int) => {
      topk(u-1)._2.toMap.getOrElse(v-1,0.0)
    }

  }

  //partitions the matrix according to a given set of rows

  def extract_columns(mat: CSCMatrix[Double], partition: Set[Int]) : CSCMatrix[Double] = {
    val builder = new CSCMatrix.Builder[Double](mat.rows,mat.cols) 
      for ((k,v) <- mat.activeIterator) {
        val row = k._1
        val col = k._2
        if (partition.contains(row)) {
          builder.add(row,col,v)
          }  
    }
  builder.result()
  }

  def similarity_test_b(train: CSCMatrix[Double], k: Int, partition : Set[Int]) : ((Int,Int) => Double) = { 
  // déjà sur le noeud
  //on pourrait preprocess le train sur le master node et broadcast les valeurso obtenues
  //preprocess est une CSC Matrix 
    //var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)
    
    //we zero the elements that are not in the partition
    for ((k,v) <- train.activeIterator) {
        val user = k._1
        if (!partition.contains(user+1)) {
          train.update(k._1,k._2,0.0)
          //println(k._1)
        }  
    }

    var (user_avg, global_avg, normalized_deviation, preprocessed_train, nbr_rows, nbr_cols) = preprocess_data_exact(train)
  
    //var preprocessed_train = preprocessed_data.value

    def topk(u: Int) = {
        val similarity = {
          val s = ((preprocessed_train)*(preprocessed_train(u,0 to preprocessed_train.cols-1).t))
          s(u) = 0.0
          s
        } 
      (u,argtopk(similarity,k).map(v => (v,similarity(v))))
    }

    (u:Int, v:Int) => {
      topk(u-1)._2.toMap.getOrElse(v-1,0.0)
    }

  }

  def similarity_test_final(train: CSCMatrix[Double], k: Int, partitionedUsers : Seq[Set[Int]], sc: SparkContext) : ((Int,Int) => Double) = {
    var myrdd = sc.parallelize(partitionedUsers)
    var rdd2 = myrdd.map(partition => similarity_test_b(train,k,partition))
    (u: Int, v:Int) => rdd2.map(f => f(u,v)).max()
  }

  def sim_builder(train: CSCMatrix[Double], k:Int, partitionedUsers : Seq[Set[Int]], sc : SparkContext) : Any = {
    //problem is that preprocessed is not a CSC matrix! preprocessing has to be done on the node itself..
    var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)
    sc.parallelize(partitionedUsers).map(partition => extract_columns(train,partition))
  }

  def nodeSimilarity(train: CSCMatrix[Double], partition: Set[Int], k: Int) : Map[Int,List[(Int,Double)]] = {
    var (_,_,_,similarity,_,_) = preprocess_data(train) //here we only look at the similarity
    var nbr_rows = train.rows
    var nbr_cols = train.cols
    val builder = new CSCMatrix.Builder[Double](nbr_rows,nbr_cols) 

    var partitionList = partition.toList

    /*
    for (row <- 0 to nbr_rows-1) {
      for (col <- argtopk(similarity(row), k)){
        builder.add(row,col,similarity(row)(col))
      } 
    } 
    val similarityMatrix = builder.result()
    */
   //line below works 100% 
   //var nodeMap = for (user <- partitionList; col <- argtopk(similarity(user),k)) yield (user,col,similarity(user)(col)) 
   //le argtopk pourrait prendre le user même en compte, donc prendre les k+1 plus grands et filtrer si un user 
   //est égal à soi même. Devrait fonctionner avec un counter
   //ici en bas avec un counter
   //var nodeMap = for (user <- partitionList; col <- argtopk(similarity(user),k+1)) yield (user,col,similarity(user)(col))

   
   var nodeMap : List[(Int,Int,Double)] = Nil
   for (user <- partitionList) {
        var counter = 0
        for {col <- argtopk(similarity(user),k+1)} {
          if (col != user && counter < k){
            counter += 1
            nodeMap = (user,col,similarity(user)(col)) :: nodeMap
          }     
        }
   }
   
  /*
  for {i <- List(1,2,3,4)} {
    var counter = 0
    for {j <- List(1,2,3,4)}{
        if (i != 1 && counter < 2) {
            ls = i :: ls
        }
    }
}
*/


   /*
   for (i <- List(1,2)) {
    var counter1 = 0.0
    for {j <- List(1,2,3,4) if (j != 1)} yield {
    counter1 =  counter1 + 1
    println(counter1)
    2*j
    }   
}
   */
   nodeMap.groupBy(_._1).map{case (k,v) => k -> v.map(el => (el._2,el._3))}

  }

  def approximateKNNSimilarity(train: CSCMatrix[Double], partitionedUsers: Seq[Set[Int]], k:Int, sc: SparkContext) : (Int,Int) => Double = {
    var trainBroadcasted = sc.broadcast(train) //could maybe be changed but I see no other way right now
    var collectedKNN = sc.parallelize(partitionedUsers).map(part => {
      var partitionedTrain = extract_columns(trainBroadcasted.value,part)
      nodeSimilarity(partitionedTrain,part,k)
    }).collect() //this is an array of #partition maps [Int,[Int,Double]]

    //we merge the maps together and remove duplicates that might be in the map 
    var reducedKNN = collectedKNN.reduce((map1,map2) => map1 ++ map2.map{case (key,v) => key -> (v ++ map1.getOrElse(key,Nil))}).map{case(key,v) => key -> v.toSet.toList}
    var kLargest : Map[Int,List[(Int, Double)]]= reducedKNN.map{case (key,v) => key -> findKLargestElements(k)(v)}
    (user1: Int, user2: Int) => {
      if (user1 == user2) 0.0 
      var similarity = kLargest(user1-1).filter(el => el._1 == user2-1)
      if (similarity.isEmpty) 0.0 else similarity.head._2
    }
    
  }

    def approximateKNNtopK(trainBroadcasted: org.apache.spark.broadcast.Broadcast[breeze.linalg.CSCMatrix[Double]], partitionedUsers: Seq[Set[Int]], k:Int, sc: SparkContext) : Map[Int,List[(Int, Double)]] = {
    //var trainBroadcasted = sc.broadcast(train) //could maybe be changed but I see no other way right now
    var collectedKNN = sc.parallelize(partitionedUsers).map(part => {
      var partitionedTrain = extract_columns(trainBroadcasted.value,part)
      nodeSimilarity(partitionedTrain,part,k)
    }).collect() //this is an array of #partition maps [Int,[Int,Double]]

    //we merge the maps together and remove duplicates that might be in the map 
    var reducedKNN = collectedKNN.reduce((map1,map2) => map1 ++ map2.map{case (key,v) => key -> (v ++ map1.getOrElse(key,Nil))}).map{case(key,v) => key -> v.toSet.toList}
    var kLargest : Map[Int,List[(Int, Double)]] = reducedKNN.map{case (key,v) => key -> findKLargestElements(k)(v)}
    kLargest
  }

    def insert(elem:(Int,Double), list : List[(Int,Double)]) : List[(Int,Double)] = list match {
    case x :: xs if x._2 < elem._2 => x :: insert(elem, xs)
    case _ => elem :: list
  }

  def findKLargestElements(k:Int)(list : List[(Int,Double)]): List[(Int,Double)] = {
      list.foldLeft(List.empty[(Int,Double)]) {(acc: List[(Int,Double)], elem:(Int,Double))=>
          val newAcc = insert(elem, acc)
          if (newAcc.size > k) newAcc.tail else newAcc
      }
  }
 

/*
      val similarity_sparse = new CSCMatrix.Builder[Double](rows=nbr_rows, cols=nbr_cols) 
    for (row <- 0 to nbr_rows-1) {
      for (col <- argtopk(similarity(row), k)){
        similarity_sparse.add(row,col,similarity(row)(col))
      } 
    } 

*/

  def predict_approximate(train: CSCMatrix[Double], k: Int, partitionedUsers: Seq[Set[Int]], sc: SparkContext) : ((Int,Int) => Double) = {

    var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)

    val br = sc.broadcast(preprocessed)

    
    
  // Predictions
    val train_broadcast = sc.broadcast(train)
    val sim  = approximateKNNtopK(train_broadcast,partitionedUsers,k,sc)
    val normalized_deviation_broadcast = sc.broadcast(normalized_deviation)
    val sim_broadcast = sc.broadcast(sim)

    def deviation_(u: Int, i: Int) : Double = {
      val train_broadcasted = train_broadcast.value
      val normalized_deviation_broadcasted = normalized_deviation_broadcast.value
      val topks = sim_broadcast.value

      val denum = topks(u).map{case (v,s) => if (train_broadcasted(v,i) == 0.0 || s == 0.0) 0.0 else (math.abs(s))}.sum
      val num = topks(u).map{case (v,s) => if (train_broadcasted(v,i) == 0.0 || s == 0.0) 0.0 else (normalized_deviation_broadcasted(v,i)*s)}.sum

      {if (denum == 0.0) 0.0 else num/denum}
    }

    (u:Int, i:Int) => {
      // element (user or movie) out of the matrix boundaries
      if (u>nbr_rows || i>nbr_cols)
        global_avg
      // rating already exists 
      else if (train(u-1,i-1) != 0.0)
        train(u-1,i-1)
      else {
        /// 
        val dev = deviation_(u-1, i-1)
        val mean = user_avg(u-1) 
        mean+dev*scale(mean+dev,mean)
      }
    }
  } 

  def mae_approximate(train: CSCMatrix[Double], test: CSCMatrix[Double], k: Int, partitionedUsers: Seq[Set[Int]], sc: SparkContext) : Double = {

    var predictor = predict_approximate(train,k,partitionedUsers,sc)
    var mae = 0.0

    for ((k,v) <- test.activeIterator) {
      val row = k._1
      val col = k._2
      mae += math.abs(predictor(row+1,col+1)-v)
    }
    
    mae/test.activeSize.toDouble
  }






    
  def similarity_k_exact(train: CSCMatrix[Double], k: Int, partition : Set[Int], sc: SparkContext) : ((Int,Int) => Double) = {
    var (user_avg, global_avg, normalized_deviation, preprocessed, nbr_rows, nbr_cols) = preprocess_data_exact(train)
    val br = sc.broadcast(preprocessed)
    
    def topk(u: Int) = {
      val similarity = {
        val s = ((br.value)*(br.value(u,0 to br.value.cols-1).t))
        s(u) = 0.0
        s
      } 
      (u,argtopk(similarity,k).map(v => (v,similarity(v))))
    }

  (u:Int, v:Int) => {
    topk(u-1)._2.toMap.getOrElse(v-1,0.0)
  }
  }

//////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////// Economics /////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////

 /*

    def knn(k: Int, train: CSCMatrix[Double],test: CSCMatrix[Double]) = {
    
    // average per user
    val user_mean = {
      // sum of ratings per user
      val numerator = (train * DenseVector.ones[Double](train.cols))
      // number of elements per user (replace 0.0 by 1.0)
      val denominator = ((train.map(elem => if (elem != 0.0) 1.0 else 0.0)) * DenseVector.ones[Double](train.cols)).map(elem => if (elem == 0.0) 1.0 else elem)
      // Elementwise division
      numerator /:/ denominator}
    
    val normalized_deviation = {
      val normalized_deviation_CSC = new CSCMatrix.Builder[Double](rows=train.rows, cols=train.cols)
      for ((k,v) <- train.activeIterator) {
        val row = k._1
        val col = k._2
        normalized_deviation_CSC.add(row, col, (v - user_mean(row))/scale(v, user_mean(row)))
      }
      normalized_deviation_CSC.result()
    }

    /// Preprocessed
    val preprocessed_denum = (normalized_deviation.map(i => i*i) * DenseVector.ones[Double](train.cols)).map(i => math.sqrt(i))

    val preprocessed  = normalized_deviation /:/ (preprocessed_denum * (DenseVector.ones[Double](train.cols).t))
    
    val similarity = (0 to train.rows-1).map(user => {
    val similarity_user = preprocessed * preprocessed(user, 0 to train.cols-1).t
    similarity_user(user) = 0.0
    similarity_user
    }) 

    /*
    val similarity_sparse = new CSCMatrix.Builder[Double](rows=train.rows, cols=train.cols) 
    for (row <- 0 to train.rows-1) {
      for (col <- argtopk(similarity(row), k)){
        similarity_sparse.add(row,col,similarity(row)(col))
      } 
    } 
    val similarity_sparse1 = similarity_sparse.result()
    */
    //println(similarity(0)(863))

    // Predictions
    val deviation = {
      val deviation_CSC = new CSCMatrix.Builder[Double](rows=test.rows, cols=test.cols)
      for ((key,value) <- test.activeIterator)
      {
        val user = key._1
        val item = key._2
        var numerator = 0.0
        var denominator = 0.0
        val temp = similarity(user)
        //val temp = similarity_sparse1(u, 0 to train.rows-1)
        for(user_temp <- argtopk(temp,k)) 
        //for((k,v) <- temp)
        { 
          if (temp(user_temp) != -inf && temp(user_temp) != 0.0 && train(user_temp,item) != 0.0 && user_temp != user){
            numerator += (temp(user_temp)*normalized_deviation(user_temp,item))
            denominator += (temp(user_temp).abs)}
        }
        deviation_CSC.add(user, item, {if (denominator != 0.0) numerator/denominator else 0.0})
      }
      deviation_CSC.result()
    }

    val prediction = {
      val prediction_CSC = new CSCMatrix.Builder[Double](rows=test.rows, cols=test.cols)
      for ((key,value) <- test.activeIterator){
        val user = key._1
        val item = key._2
        var avg_user = user_mean(user)
        var dev_user = deviation(user, item)
        prediction_CSC.add(user, item, avg_user + dev_user * scale((avg_user + dev_user), avg_user))
      }
      prediction_CSC.result()
    }

    prediction
  }


   ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

   
  def KNNSimilarity_Breeze_old(train: CSCMatrix[Double], k: Int): ((Int,Int) => Double) = {
    println(train.max)
    println(train.min)
    println(train(20, 0 to 100))
    println(train.rows)
    println(train.cols)



    //var vec = train(20, 0 to 100) * train(21, 0 to 100).t
    var elem1 = train(20, 0 to 20).t
  
    for (col <- argtopk(train(20, 0 to 20).t, 10-5)){
      println(col)
    } 
 
    println(elem1)

    val elem = train.rows -1
    
    // u : user id
    // v : user id 
    (u:Int, v:Int) => {
      // check if similarity exists in map, else output 0.0
      11111.0
      } 
  }


  def normalized_deviation_Breeze(r :Double, r_avg_u :Double): Double ={
    return (r-r_avg_u)/scale_Breeze(r,r_avg_u)
  } 
  
  def scale_Breeze(x : Double, r_avg_u : Double): Double = x compare r_avg_u match {
    case 1 => 5.0 - r_avg_u
    case -1 => r_avg_u - 1.0
    case 0 => 1.0
  } 

  // fun that given an user id returns (nbr_of_movie_rated, avg_rating) of the user 
  def count_and_avg_per_User(train: CSCMatrix[Double]): ((Int) => (Int,Double)) = {
    
    var map_sol : Map[Int,(Int, Double)] = Map()
    for ((k,v) <- train.activeIterator) {
        val old_value = map_sol.getOrElse(k._1, (0,0.0)) 
        map_sol += (k._1 -> (old_value._1 + 1, (old_value._1 * old_value._2 + v)/(old_value._1 + 1)))
    }

   // u : user id 
   (u:Int) => {
    // first element ._1 : nbr of movies rated by u 
    // second element ._2 : average rating by u 
    map_sol.getOrElse(u,(0,0.0))
    } 
  }

  
    def trainPreprocessing_Breeze(train: CSCMatrix[Double]): CSCMatrix[Double] = { 
      println("start trainPreprocessing_Breeze")
      // ok
      //val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
      val avg_count_user = count_and_avg_per_User(train)

      // for every prediction check if there is an average 
      // if yes averageUserRatings else
      // ok 
      //val normalizedTrain = train.map{ r: Rating => if (averageUserRatings isDefinedAt r.user) {Rating(r.user,r.item, normalized_deviation_Breeze(r.rating,averageUserRatings(r.user)))} else {Rating(r.user,r.item,0)}}    
      for ((k,v) <- train.activeIterator) {
        var avg_user = avg_count_user(k._1)._2
        if (avg_user != 0.0)
          train(k._1,k._2) = normalized_deviation_Breeze(v, avg_user)
        else 
          train(k._1,k._2) = 0.0
      }

      // ok 
      //val normalizedTrainGrouped = normalizedTrain.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x,y: Rating) => x + math.pow(y.rating,2))}.map{case (k,v) => k -> math.sqrt(v)}
      var map_sol : Map[Int,Double] = Map()
      for ((k,v) <- train.activeIterator) {
        val old_value = map_sol.getOrElse(k._1, 0.0) 
        map_sol += (k._1 -> math.sqrt(old_value*old_value + math.pow(v,2)))
      }


      for ((k,v) <- train.activeIterator) {
        var value = map_sol.getOrElse(k._1, 0.0) 
        if (value != 0.0)
          train(k._1,k._2) = v / value
        else 
          train(k._1,k._2) = 0.0
      }
      /*
      val trainPreprocessed = normalizedTrain.map{r: Rating => if (normalizedTrainGrouped(r.user) != 0.0) { 
        Rating(r.user,r.item,r.rating/normalizedTrainGrouped(r.user)) 
      } else {
        Rating(r.user,r.item,0.0)
      }
        }
      */
      println("end trainPreprocessing_Breeze")
      println("trainPreprocessing_Breeze",train)
      train
  }


  def KNNSimilarity_Breeze(train: CSCMatrix[Double], k: Int): ((Int,Int) => Double) = {
    println("Before",train)
    println("Cosine start")
    val trainPreprocessed = trainPreprocessing_Breeze(train)
    //val similarities = cosineSimilarity_Breeze(train)
    println("Cosine done")
    println("new KNN",trainPreprocessed)
    println("KNN",train)

    val n = train.rows
    println("nbr of rows",n)
    // create square CSC matrix (nbr of user: n)
    println("Start building")
    val builder = new CSCMatrix.Builder[Double](rows=n, cols=n)
    println("End building")
    
    println("Start for loop similarities !!!")
    // create n min_heap that contains k elements 
    // fill matrix by calling similarities for every value (u,v)

    val similarity = (0 to n-1).map(u => {
    val s = - trainPreprocessed*trainPreprocessed(u,0 to train.cols-1).t
    s(u) = 0.0
    s
    }) 
    /*
    for (row <- 0 to n-1) {
      for (col <- 0 to row) {
        if (row == (n/2).toInt && col == 2)
          println("half there!")
        if (row != col){
          //var temp_similarity = - similarities(row,col)

          /// !!!! takes a lot of time !!!!
          var temp_similarity = - trainPreprocessed(row, 0 to n) * trainPreprocessed(col, 0 to n).t
          
          
        // build symetric matrix 
          builder.add(row, col, temp_similarity)
          builder.add(col, row, temp_similarity)
          }
      } 
    } 
    */

    println("End for loop similarities")

    //val similartiy_matrix = builder.result()
    println("Start top k")
    // takes a lot of timeeee
    // for each row replace the top n-k values by 0
    for (row <- 0 to n-1) {
      for (col <- argtopk(similarity(row), n-k)){
        similarity(row)(col) = 0.0
      } 
    } 
    println("End top k")
  
    // u : user id
    // v : user id 
    (u:Int, v:Int) => {
      // return - the value 
      var sol = - similarity(u-1)(v-1)
      if (sol == -0.0)
        0.0
      else 
        sol
      } 
  }


  def KNNSimilarity_Breeze_test(train: CSCMatrix[Double], k: Int): ((Int,Int) => Double) = {
    println("Before",train)
    println("Cosine start")
    val trainPreprocessed = trainPreprocessing_Breeze(train)
    //val similarities = cosineSimilarity_Breeze(train)
    println("Cosine done")
    println("new KNN",trainPreprocessed)
    println("KNN",train)

    val n = train.rows
    println("nbr of rows",n)
    // create n min_heap that contains k elements 
    println("Start building")
    var n_minHeap = (0 to n).map(x => (x,scala.collection.mutable.PriorityQueue.empty(MinOrder))).toMap
    println("End building")
    
    println("Start for loop similarities !!!")
    // fill matrix by calling similarities for every value (u,v)
    for (row <- 0 to n-1) {
      for (col <- 0 to row) {
        if (row == (2*n/3).toInt && col == 2)
          println("half there!")
        if (row != col){
          // faire map avec user -> valeur non nul 
          // faire union des deux et faire une boucle dedans 
          val similarity_u_v = trainPreprocessed(row, 0 to n) * trainPreprocessed(col, 0 to n).t
          
          if (n_minHeap(row).size < k) {
              n_minHeap(row).enqueue((col,similarity_u_v))
          } else {
              if (similarity_u_v > n_minHeap(row).head._2)
                  n_minHeap(row).enqueue((col,similarity_u_v))
                  n_minHeap(row).dequeue 
          }

          if (n_minHeap(col).size < k) {
              n_minHeap(col).enqueue((row,similarity_u_v))
          } else {
              if (similarity_u_v > n_minHeap(col).head._2)
                  n_minHeap(col).enqueue((row,similarity_u_v))
                  n_minHeap(col).dequeue 
          }
        }
      } 
    } 

    println("End for loop similarities")

    // u : user id
    // v : user id 
    (u:Int, v:Int) => {
      // return the value 
      n_minHeap(u-1).toMap.getOrElse(v-1, 0.0)
      } 
  }


  def cosineSimilarity_Breeze(train: CSCMatrix[Double]): ((Int,Int) => Double) = {
    val trainPreprocessed = trainPreprocessing_Breeze(train)
    val nbr_rows = trainPreprocessed.rows
    (u:Int, v:Int) => {
      trainPreprocessed(u, 0 to nbr_rows) * trainPreprocessed(v, 0 to nbr_rows).t
    }
  } 

  // compute the MAE for a given k on the test set, trained on the train set
  def computeMae_k_Breeze(train: CSCMatrix[Double],test: CSCMatrix[Double], predictor: (CSCMatrix[Double],Int) => ((Int,Int) => Double), k: Int) : Double = {
    var fun_predictions = predictor(train, k)

    var sol_MAE : Double = 0.0

    for ((k,v) <- test.activeIterator) {
        sol_MAE = sol_MAE + (fun_predictions(k._1, k._2) - v).abs 
    }
    sol_MAE /(test.activeSize)
  }

 

  def predictorSimilarity_k_Breeze(train: CSCMatrix[Double], k: Int) : (Int,Int) => Double = { 
    //val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    val averageUserRatings = count_and_avg_per_User(train)
    
    val userWeighted = userSpecificWeightedSum_k_Breeze(train, k)

    // to check!
    val globalAverage = train.mean 
    
    // u : user id 
    // i : item id
    (u: Int, i: Int) => {
      var avgUser = averageUserRatings(u)
      if (avgUser != 0.0)
        val userWgt = userWeighted(u,i)
        avgUser + userWgt * scale((avgUser + userWgt),avgUser)
      else 
        globalAverage
    }
  }

  def userSpecificWeightedSum_k_Breeze(train: CSCMatrix[Double], k: Int) : (Int,Int) => Double = {
    //val averageUserRatings = count_and_avg_per_User(train)
    
    // gives back a different train but same nbr of elements
    val normalizedTrain = trainNormalized_Breeze(train) 
    // returns value for user and i 
    val finder = findInTrain(normalizedTrain)

    val similarities = KNNSimilarity(train,k)
    val itemsWatchedBy = train.groupBy(_.item).map{case (k,v) => k -> v.map(r => r.user)}

    // u : user id 
    // i : item id
    (u: Int, i: Int) => {
      // prendre tous les gens qui ont vu film i 
      // calculer similarities avec u 
      // additioner tous les similarities et multiplier par value de train modifié 

      // diviser par abs sum de similarities 
      // retourner 1 / 2 

      val col_i = train(0 to train.rows, i-1)




      if (itemsWatchedBy isDefinedAt i) {
        var output = itemsWatchedBy(i).foldLeft((0.0,0.0))((acc,usr) => {
        var similarity_u_usr = similarities(u,usr)
  
        (acc._1 + similarity_u_usr*finder(usr,i),acc._2 + similarity_u_usr.abs)})

        if (output._2 == 0) 0.0 else {output._1/output._2}
      } else 0.0
    }
  }

  def findInTrain_Breeze(train: CSCMatrix[Double]) : (Int,Int) => Double = {
    val trainGrouped = train.groupBy(_.user)
    (u: Int, i: Int) => {
      if (trainGrouped(u).filter(r => r.item == i).isEmpty) -999 else {trainGrouped(u).filter(r => r.item == i).head.rating}
    }
  }

  def trainNormalized_Breeze(train: CSCMatrix[Double]) :CSCMatrix[Double] = {
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    train.map{ r: Rating => if (averageUserRatings isDefinedAt r.user) {Rating(r.user,r.item,normalized_deviation(r.rating, averageUserRatings(r.user)))} else {Rating(r.user,r.item,0)}}
  }

 


  
    // returns the list of movies that two users have in common (both rated the movie) 
  def commonMovies_Breeze(train: CSCMatrix[Double], u: Int, v: Int): Seq[Int] = {
      // create empty list
      var commonMovies_List : List[Int] = List()
      for (i <- 0 to train.rows-1){
        if (train(u, i)>0.0)
          if (train(v, i)>0.0)
            // update movie index to list 
            commonMovies_List = commonMovies_List :+ i 
        }
      commonMovies_List
  }

    /*
  def avg_user(train :CSCMatrix[Double]): (DenseVector[Double], Double) = {
    // sum of ratings per user
    val numerator = (train * DenseVector.ones[Double](train.cols))
    // number of elements per user (replace 0.0 by 1.0)
    val denominator = ((train.map(elem => if (elem != 0.0) 1.0 else 0.0)) * DenseVector.ones[Double](train.cols)).map(elem => if (elem == 0.0) 1.0 else elem)
    
    // average per user
    val user_mean = {
      // Elementwise division
      numerator /:/ denominator}
    
    // global average 
    val global_avg = sum(numerator) / sum(denominator)
  }

  // calculer le plus rapide pour avoir moyenne 
  def user_Ju(train: CSCMatrix[Double]): ((Int) => (Double)) = {
    
    var map_sol : Map[Int,(Int, Double)] = Map()
    for ((k,v) <- train.activeIterator) {
        val old_value = map_sol.getOrElse(k._1, (0,0.0)) 
        map_sol += (k._1 -> (old_value._1 + 1, (old_value._1 * old_value._2 + v)/(old_value._1 + 1)))
    }

   // u : user id 
   (u:Int) => {
    // first element ._1 : nbr of movies rated by u 
    // second element ._2 : average rating by u 
    map_sol.getOrElse(u,(0,0.0))._2
    } 
  }
  
  def user_Raph(train: CSCMatrix[Double]): ((Int) => (Double)) = {
    val user_mean = {
      val denum = ((train.map(i => if( i != 0.0) 1.0 else 0.0))*DenseVector.ones[Double](train.cols))
      val num = (train*DenseVector.ones[Double](train.cols)).map(i => if (i == 0.0) 0.0 else i)
      num/:/denum}
    (u:Int) => {
      user_mean(u)
    }
  }

  */


  */


}


