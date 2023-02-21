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


var partitionedUsers : Seq[Set[Int]] = partitionUsers(
      943, 
      10, 
      2 
)

println(partitionedUsers(1))

var mylist = List(1,1,2,2)
var mylist1 = List(1,2,3)

def dummy(i:Int, j : Int) : Int = 2*i+j
def foo(i: Int): Int = 2*i


var yaa = for (i <- mylist; j <- mylist1) yield (i,dummy(i,j))
yaa.groupBy(_._1)

var mylist2 = List(1,2,3)
var b = List((1,2,3),(1,3,4),(2,4,5))
var c = b.groupBy(_._1).map{case (k,v) => k -> v.map(el => (el._2,el._3))}
println(c)

var mymap3 = Map(1 -> 2, 2 -> 3)
var mymap4 = Map(1 -> 3, 3 -> 1)

var fmap = (mymap3.toList ++ mymap4.toList).groupBy(_._1).map{case (k,v) => k -> v.map(el => el._2)}
fmap.getOrElse(1,0)
fmap(1)

var ls : List[Int] = Nil
var counter = 0


for {i <- List(1,2,3,4) if (i != 1)} {
    counter += 1
    2*i}
print(counter)

for {i <- List(1,2,3,4)} {
    var counter = 0
    for {j <- List(1,2,3,4)}{
        if (i != 1 && counter < 2) {
            counter += 1
            ls = i :: ls
        }
    }
}

print(ls)


  



