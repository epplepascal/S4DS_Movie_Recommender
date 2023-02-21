package shared
import scala.collection.SortedSet
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

package object predictions
{
  case class Rating(user: Int, item: Int, rating: Double)
  
  /////////////////////////////////////////////////////////////////
  // General functions
 
  def timingInMs(f : () => Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0
  
  // mean with foldLeft apply on a Seq of Rating
  def mean_rating(train: Seq[Rating]): Double ={
    train.foldLeft(0.0)(_+_.rating) / train.length 
  }

  def normalized_deviation(r :Double, r_avg_u :Double): Double ={
    return (r-r_avg_u)/scale(r,r_avg_u)
  } 

  def load(spark : org.apache.spark.sql.SparkSession,  path : String, sep : String) : org.apache.spark.rdd.RDD[Rating] = {
       val file = spark.sparkContext.textFile(path)
       return file
         .map(l => {
           val cols = l.split(sep).map(_.trim)
           toInt(cols(0)) match {
             case Some(_) => Some(Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble))
             case None => None
           }
       })
         .filter({ case Some(_) => true 
                   case None => false })
         .map({ case Some(x) => x 
                case None => Rating(-1, -1, -1)})
  }

  // function that replicates equation (3)
  def scale(x : Double, r_avg_u : Double): Double = x compare r_avg_u match {
    case 0 => 1.0
    case 1 => 5.0 - r_avg_u
    case -1 => r_avg_u - 1.0
  } 

  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else {
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble)
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def compute_item_i_average(train: Seq[shared.predictions.Rating],item: Int) : Double = {
    var U_i = train.filter(x => (x.item == item))
    
    // if item does not exist in the train set 
    if (U_i.isEmpty)
      return mean_rating(train)
    else 
      return mean_rating(U_i)
  }

  def compute_user_i_average(train: Seq[shared.predictions.Rating],user: Int) : Double = {
    // mean of the ratings of a specific user 
    return mean_rating(train.filter(x => (x.user == user)))
  }

  def compute_item_i_average_deviation(train: Seq[shared.predictions.Rating], item: Int) : Double = {
      var U_i = train.filter(x => x.item == item)

      // if item does not exist in the train set 
      if (U_i.isEmpty)
        return 0.0
      else 
        return U_i.map(x => normalized_deviation(x.rating, compute_user_i_average(train, x.user))).sum/ U_i.length
  }

 //Compute global average with RDDs
  def globalAverage(train: RDD[Rating]) : Double = {
    mean(train.map(r => r.rating))
  }

  //Compute the item average of a given item - error message if item not in data set.
  def itemAverage(train: RDD[Rating]) : Int => Any = {
    val averageItemRatings = train.groupBy(_.item).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/v.size}
    (i: Int) => if (!(averageItemRatings.filter(user => user._1 == i).isEmpty())) averageItemRatings.filter(item => item._1 == i).map(_._2).first() else {println("not in data set")}
  }

  //Compute the average of the ratings of a given user - error message if user not in data set.
  def userAverage(train: RDD[Rating]): Int => Any = {
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/v.size}
    (u: Int) => if (!(averageUserRatings.filter(user => user._1 == u).isEmpty())) {averageUserRatings.filter(user => user._1 == u).map(_._2).first()} else {println("not in data set")}
  }

  // returns the list of movies that two users have in common (both rated the movie) 
  def commonMovies(train: Seq[Rating]): ((Int,Int) => Seq[Int] ) = {
    val userMovies = train.groupBy(_.user).map{case (k,v) => k -> v.map(r => r.item)}
    (u:Int, v:Int) => userMovies(u).intersect(userMovies(v))
  }

  def trainPreprocessing(train: Seq[Rating]): Seq[Rating] = { //faut revenir dessus mais ça me parait juste (d'après tgrm c'est bon)
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    val normalizedTrain = train.map{ r: Rating => if (averageUserRatings isDefinedAt r.user) {Rating(r.user,r.item,normalized_deviation(r.rating,averageUserRatings(r.user)))} else {Rating(r.user,r.item,0)}}
    val normalizedTrainGrouped = normalizedTrain.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x,y: Rating) => x + math.pow(y.rating,2))}.map{case (k,v) => k -> math.sqrt(v)}
    val trainPreprocessed = normalizedTrain.map{r: Rating => if (normalizedTrainGrouped(r.user) != 0.0) { 
      Rating(r.user,r.item,r.rating/normalizedTrainGrouped(r.user)) 
    } else {
      Rating(r.user,r.item,0.0)
    }
      }
    trainPreprocessed
  }

  def trainNormalized(train: Seq[Rating]) : Seq[Rating] = {
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    train.map{ r: Rating => if (averageUserRatings isDefinedAt r.user) {Rating(r.user,r.item,normalized_deviation(r.rating,averageUserRatings(r.user)))} else {Rating(r.user,r.item,0)}}
  }

  def findInTrain(train: Seq[Rating]) : (Int,Int) => Double = {
    val trainGrouped = train.groupBy(_.user)
    (u: Int, i: Int) => {
      if (trainGrouped(u).filter(r => r.item == i).isEmpty) -999 else {trainGrouped(u).filter(r => r.item == i).head.rating}
    }
  }

  // compute the MAE on the predicted test ratings, trained on the train set given a predictor  
  def computeMae(train: Seq[Rating],test: Seq[Rating],predictor: Seq[Rating] => ((Int,Int) => Double)) : Double = {
    var fun_predictions = predictor(train)
    test.foldLeft(0.0)((x, y: Rating) => (x + (fun_predictions(y.user,y.item) - y.rating).abs))/ test.length
  }

  /////////////////////////////////////////////////////////////////
  // RDD functions 

  // compute mean 
  def mean(s : RDD[Double]): Double =  if (s.count > 0) s.reduce(_+_) / s.count else 0.0

  // Global Average predictor implemented with RDDs 
  def predictorGlobalAverage(train: RDD[Rating]) : (Int,Int) => Double = {
    val globalAverage = mean(train.map(r => r.rating))

    // u : user id 
    // i : item id 
    (u: Int, i:Int) => globalAverage
  }

 // User Average predictor implemented with RDDs 
  def predictorUserAverage(train: RDD[Rating]) : ((Int,Int) => Double) = {
    val globalAverage = mean(train.map(x=>x.rating))
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.size}
    
    // u : user id 
    // i : item id 
    (u: Int, i: Int) => if (!(averageUserRatings.filter(user => user._1 == u).isEmpty())) {averageUserRatings.filter(user => user._1 == u).map(_._2).first()} else globalAverage 
  }

 // Item Average predictor implemented with RDDs 
  def predictorItemAverage(train: RDD[Rating]) : ((Int,Int) => Double) = {
    val globalAverage = mean(train.map(x => x.rating))
    val averageItemRatings = train.groupBy(_.item).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.size}
    
    // u : user id 
    // i : item id 
    (u: Int, i:Int) => if (!(averageItemRatings.filter(item => item._1 == i).isEmpty())) {averageItemRatings.filter(item => item._1 == i).map(_._2).first()} else globalAverage
  }

  // Takes as parameters a train set and an item, and return the average deviation of this item
  def compute_item_i_average_deviation(train: RDD[Rating], item: Int) : Double = {
      var U_i = train.filter(x => x.item == item)
      
      // if item does not exist in the train set 
      if (U_i.isEmpty)
        return 0.0
      else 
        return U_i.map(x => normalized_deviation(x.rating, compute_user_i_average(train, x.user))).sum/ U_i.count
  }
  
  // Takes as parameters a train set and a user, and returns the average rating of user i
  def compute_user_i_average(train: RDD[Rating],user: Int) : Double = {
    val x = train.filter(x => (x.user == user))
    return mean(x.map(x => x.rating))
  }

  // Baseline predictor implemented with RDDs, returns a function which given a user u and and item i
  // returns the (baseline) predicted value for user u on item i.
  def predictorBaseline(train: RDD[Rating]) : ((Int,Int) => Double) = {
    val globalAverage = mean(train.map(x => x.rating)) 
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.size}.collectAsMap
    val stdDevItem1 = averageDeviation(train)

    val stdDevItem = train.groupBy(_.item).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => { 
      var term = averageUserRatings.getOrElse(y.user, globalAverage) 
      (x + (y.rating - term)/scale(y.rating,term))})/ v.size.toDouble}.collectAsMap
  
    // u : user id 
    // i : item id 
    (u: Int, i: Int) =>      
      if (stdDevItem isDefinedAt i) {
        if (averageUserRatings isDefinedAt u) {
          averageUserRatings(u) + stdDevItem(i) * scale(averageUserRatings(u) + stdDevItem(i), averageUserRatings(u))
        } else {
          globalAverage
        } 
      } else { 
        averageUserRatings.getOrElse(u, globalAverage)
      } 
  }

  // Computation of the MAE with RDDs
  def computeMaeRDD(train: RDD[Rating],test: RDD[Rating],predictor: RDD[Rating] => ((Int,Int) => Double), session: SparkSession) : Double = {
    var predictions = predictor(train)
    var prediction : Broadcast[(Int, Int) => Double] = session.sparkContext.broadcast(predictor(train))
    var output = test.map(r => (prediction.value(r.user,r.item)-r.rating).abs)
    
    output.sum/output.count()
  }

  // Returns a function to compute the average deviation of an item i (implemented with RDDs)
  def averageDeviation(train: RDD[Rating]): Int => Double = {
    val globalAverage = mean(train.map(x=>x.rating))
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.size}.collectAsMap

    val stdDevItem = train.groupBy(_.item).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => { //peut être en faire une fonction
      var term = globalAverage
      if (averageUserRatings isDefinedAt y.user){
        term = averageUserRatings(y.user)
      }
      (x + (y.rating - term)/scale(y.rating,term))})/ v.size}

    // i : item id 
    (i: Int) => {
      val itemFilter = stdDevItem.filter(x => x._1 == i)
      if (itemFilter.isEmpty()) 0.0 else itemFilter.map(_._2).first()
    }
  }

  /////////////////////////////////////////////////////////////////
  // Predictor 

  def predictorGlobalAverage(train: Seq[Rating]) : (Int,Int) => Double = {
    val globalAverage = mean_rating(train)

    // u : user id 
    // i : item id 
    (u: Int, i: Int) => globalAverage
  }

  def predictorUserAverage(train: Seq[Rating]) : ((Int,Int) => Double) = {
    val globalAverage = mean_rating(train)
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    
    // u : user id 
    // i : item id 
    (u: Int, i: Int) =>  averageUserRatings.getOrElse(u, globalAverage)
  }

  def predictorItemAverage(train: Seq[Rating]) : ((Int,Int) => Double) = { 
    val globalAverage = mean_rating(train)
    val averageItemRatings = train.groupBy(_.item).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    
    // u : user id 
    // i : item id 
    (u: Int, i: Int) => averageItemRatings.getOrElse(i, globalAverage)
  }

  
 def predictorBaseline(train: Seq[Rating]) : ((Int,Int) => Double) = {
    val globalAverage = mean_rating(train) 
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    val stdDevItem = train.groupBy(_.item).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => { 
      var term = averageUserRatings.getOrElse( y.user, globalAverage) 
      (x + (y.rating - term)/scale(y.rating,term))})/ v.length}
      
    // u : user id 
    // i : item id 
    (u: Int, i: Int) => 
      // check if item i is defined in Map stdDevItem   
      if (stdDevItem isDefinedAt i) {
        // check if user u is defined in Map averageUserRatings   
        if (averageUserRatings isDefinedAt u) {
          averageUserRatings(u) + stdDevItem(i) * scale(averageUserRatings(u) + stdDevItem(i), averageUserRatings(u))
        } else {
          globalAverage
        } 
      } else { 
        averageUserRatings.getOrElse(u, globalAverage)
      } 
  }


  def predictorSimilarityCosine(train: Seq[Rating]) : (Int,Int) => Double = { 
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    val userWeighted = userSpecificWeightedSum(train)
    val globalAverage = mean_rating(train)
    
    // u : user id 
    // i : item id 
    (u: Int, i: Int) => {
      // check if user u is defined in Map averageUserRatings   
      if (averageUserRatings isDefinedAt u) {
        val avgUser = averageUserRatings(u)
        val userWgt = userWeighted(u,i)
        avgUser + userWgt * scale((avgUser + userWgt),avgUser)
      } else globalAverage
    }
  }

  def predictorSimilarityUniform(train: Seq[Rating]) : (Int,Int) => Double = { 
    val globalAverage = mean_rating(train) 
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    val userWeighted = userSpecificWeightedSumUniform(train)
    
    // u : user id 
    // i : item id 
    (u: Int, i: Int) => {
      if (averageUserRatings isDefinedAt u) {
        val avgUser = averageUserRatings(u)
        val userWgt = userWeighted(u,i)
        avgUser + userWgt * scale((avgUser + userWgt),avgUser)
      } else globalAverage
    }
  }

  def predictorSimilarityJaccard(train: Seq[Rating]) : (Int,Int) => Double = { 
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    val userWeighted = userSpecificWeightedSumJaccard(train)
    val globalAverage = mean_rating(train)
    
    // u : user id 
    // i : item id 
    (u: Int, i: Int) => {
      if (averageUserRatings isDefinedAt u) {
        val avgUser = averageUserRatings(u)
        val userWgt = userWeighted(u,i)
        avgUser + userWgt * scale((avgUser + userWgt),avgUser)
      } else globalAverage
    }
  }

  /////////////////////////////////////////////////////////////////
  // similarity 

  def jaccardSimilarity(train: Seq[Rating]) : ((Int, Int) => Double) = {
    // Map : user u -> List(items i of the movies that user u watched/rated)
    val usersWatched = train.groupBy(_.user).map{case (k,v) => k -> v.map(r => r.item)} 
  
    // u : user id 
    // i : item id 
    (u:Int, v:Int) => {
      var user1 = usersWatched(u)
      var user2 = usersWatched(v)
      var num = user1.intersect(user2).toSet.size.toDouble
      var denum = user1.union(user2).toSet.size.toDouble
      
      if (denum == 0.0) 
        {0.0}
      else 
        {num/denum} 
    }
  }

  def cosineSimilarity(train: Seq[Rating]): ((Int,Int) => Double) = {
    val trainPreprocessed = trainPreprocessing(train)
    val trainPreprocessedUser = trainPreprocessed.groupBy(_.user)
    val common = commonMovies(train)

    // u : user id 
    // i : item id
    (u:Int, v:Int) => {
    val user1 = trainPreprocessedUser(u)
    val user2 = trainPreprocessedUser(v)
    // list of movies that user u and v both rated
    var common_u_v = common(u,v)

    if (common_u_v.isEmpty) 
      {0.0} 
    else {
      common_u_v.foldLeft(0.0)((x, y: Int) => (x + user1.find(r => r.item == y).head.rating * user2.find(r => r.item == y).head.rating))
      }
    }
  } 

  /////////////////////////////////////////////////////////////////
  // weighted sum  

   def userSpecificWeightedSumUniform(train: Seq[Rating]) : (Int,Int) => Double = {

    val normalizedTrain = trainNormalized(train)
    val finder = findInTrain(normalizedTrain)
    val itemsWatchedBy = train.groupBy(_.item).map{case (k,v) => k -> v.map(r => r.user)}

    // u : user id 
    // i : item id
    (u: Int, i: Int) => {
      if (itemsWatchedBy isDefinedAt i) {
        var output = itemsWatchedBy(i).foldLeft((0.0,0.0))((acc,usr) => {
        var sims = 1.0 // uniform similarites, you need no specific value to compute

        (acc._1 + sims*finder(usr,i),acc._2 + sims.abs)})

        if (output._2 == 0) 0.0 else {output._1/output._2}
      } else 0.0
    }
  }

  def userSpecificWeightedSumJaccard(train: Seq[Rating]) : ((Int,Int) => Double) = {
    // retrieve the highest id user 
    var max_user = train.map(x => x.user).max.toInt
    val normalizedTrain = trainNormalized(train)
    val finder = findInTrain(normalizedTrain)
    val similarities = jaccardSimilarity(train)
    val itemsWatchedBy = train.groupBy(_.item).map{case (k,v) => k -> v.map(r => r.user)}

    val sol = Array.tabulate(max_user)(u => Array.tabulate(max_user)(v => if (u>=v) {similarities(u+1,v+1)} else 0.0 ))

    // u : user id 
    // i : item id
   (u: Int, i: Int) => {
      if (itemsWatchedBy isDefinedAt i) {
        var output = itemsWatchedBy(i).foldLeft((0.0,0.0))((acc,usr) => {
        var sims = 0.0
        if (u < usr) {sims = sol(usr-1)(u-1)}
        else {sims = sol(u-1)(usr-1)}

        (acc._1 + sims*finder(usr,i),acc._2 + sims.abs)})

        if (output._2 == 0) 0.0 else {output._1/output._2}
      } else 0.0
    }
  }

  def userSpecificWeightedSum(train: Seq[Rating]) : (Int,Int) => Double = {
    // retrieve the highest id user 
    var max_user = train.map(x => x.user).max.toInt
    val normalizedTrain = trainNormalized(train)
    val finder = findInTrain(normalizedTrain)
    val similarities = cosineSimilarity(train)
    val itemsWatchedBy = train.groupBy(_.item).map{case (k,v) => k -> v.map(r => r.user)}
    val sol = Array.tabulate(max_user)(u => Array.tabulate(max_user)(v => if (u>=v) {similarities(u+1,v+1)} else 0.0 ))

    // u : user id 
    // i : item id
    (u: Int, i: Int) => {
      if (itemsWatchedBy isDefinedAt i) {
        var output = itemsWatchedBy(i).foldLeft((0.0,0.0))((acc,usr) => {
        var sims = 0.0
        if (u < usr) {sims = sol(usr-1)(u-1)}
        else {sims = sol(u-1)(usr-1)}

        (acc._1 + sims * finder(usr,i),acc._2 + sims.abs)})

        if (output._2 == 0) 0.0 else {output._1/output._2}
      } else 0.0
    }
  }

  /////////////////////////////////////////////////////////////////
  // k-nn 

  // compute the MAE for a given k on the test set, trained on the train set
  def computeMae_k(train: Seq[Rating],test: Seq[Rating],predictor: (Seq[Rating],Int) => ((Int,Int) => Double),k:Int) : Double = {
    var fun_predictions = predictor(train,k)
    test.foldLeft(0.0)((x, y: Rating) => (x + (fun_predictions(y.user,y.item) - y.rating).abs))/ test.length
  }

  def predictorSimilarity_k(train: Seq[Rating], k: Int) : (Int,Int) => Double = { 
    val averageUserRatings = train.groupBy(_.user).map{case (k,v) => k -> v.foldLeft(0.0)((x, y: Rating) => (x + y.rating))/ v.length}
    val userWeighted = userSpecificWeightedSum_k(train, k)
    val globalAverage = mean_rating(train) 
    
    // u : user id 
    // i : item id
    (u: Int, i: Int) => {
      if (averageUserRatings isDefinedAt u) {
        val avgUser = averageUserRatings(u)
        val userWgt = userWeighted(u,i)
        avgUser + userWgt * scale((avgUser + userWgt),avgUser)
      } else globalAverage
    }
  }

  def userSpecificWeightedSum_k(train: Seq[Rating], k: Int) : (Int,Int) => Double = {
    val normalizedTrain = trainNormalized(train)
    val finder = findInTrain(normalizedTrain)
    val similarities = KNNSimilarity(train,k)
    val itemsWatchedBy = train.groupBy(_.item).map{case (k,v) => k -> v.map(r => r.user)}

    // u : user id 
    // i : item id
    (u: Int, i: Int) => {
      if (itemsWatchedBy isDefinedAt i) {
        var output = itemsWatchedBy(i).foldLeft((0.0,0.0))((acc,usr) => {
        var similarity_u_usr = similarities(u,usr)
  
        (acc._1 + similarity_u_usr*finder(usr,i),acc._2 + similarity_u_usr.abs)})

        if (output._2 == 0) 0.0 else {output._1/output._2}
      } else 0.0
    }
  }
  
  def KNNSimilarity(train: Seq[Rating], k: Int): ((Int,Int) => Double) = {
    // retrieve the highest id user 
    var max_user = train.map(x => x.user).max.toInt
    val similarities = cosineSimilarity(train)
    // compute and store in a half matrix the similarity values of all the combination of (user,user)
    val half_matrix = Array.tabulate(max_user)(u => Array.tabulate(u)(v => similarities(u+1,v+1)))
    
    // fill a matrix of size (max_user,max_user-1) (without the diagonal u == v)
    // -> allows to take advantage of the symmetry of the problem : similarity(u,v) == similarity(v,u)
    val full_matrix_k_elements = Array.tabulate(max_user)(u => Array.tabulate(max_user-1)(v => 
      if (u == v){
              (v+1,half_matrix(u+1)(v))}
      else if (u < v){
              (v+1,half_matrix(v+1)(u))
      } else { 
              (v,half_matrix(u)(v))}).sortBy(_._2).drop(max_user-1-k).toMap) // sort by the similarity and keep only the k elements 
    
    // u : user id
    // v : user id 
    (u:Int, v:Int) => {
      // check if similarity exists in map, else output 0.0
      full_matrix_k_elements(u-1).getOrElse(v-1,0.0)
      } 
  }

  // returns a function that can output the n top predicted rank movies for user u  
  def best_i_item_for_user_u(data: Seq[Rating], predictor: (Seq[Rating], Int) => ((Int,Int) => Double), k: Int) : ((Int,Int) => Seq[(Int,Double)]) = {
    var fun_predictions = predictor(data,k)
    
    // u : user id
    // n : number of top n prediction to output  
    (u:Int, n:Int) => { 
      // collect item that user u predicted and make a unique list (set)
      var item_graded_by_u = data.filter(x => x.user == u).map(x => x.item).toSet
      // collect all the items and make a unique list (set)
      var item_available = data.map(x => x.item).toSet
      // keep only the items that are not predicted by user u 
      var item_to_predict = item_available.diff(item_graded_by_u)

      // predict for each item, sort by prediction (descending) and sort by id (ascending) 
      // take the first n (n highest graded prediction)
      item_to_predict.map(item => (item,fun_predictions(u,item))).toSeq.sortWith(_._1 < _._1).sortWith(_._2 > _._2).take(n)
    } 
  }
    
}

