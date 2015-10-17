import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

	  
    val rdd = sc.textFile("/home/islam/spark-training-master/examples/pairs/pagerank/input")
 
 	val rdd2 = rdd.map { x => x.split(": ") }
  	val rdd5 = rdd2.map( x => (x(0) , x(1).split(", ")) )
  	val rdd3 = rdd2.map(x => (x(0), 1.0))

  	
    val costs = rdd5.map { x => (x._1, 1.0 / x._2.length) }
//  	val rdd6 = rdd3.join(rdd5).join(costs)
 // 	val currentValues = rdd6.map{ x =>  (x._1, x._2._1._1 - (x._2._2 * x._2._1._2.length))  }
  	val rdd7 = rdd5.join(costs).values
  	val rdd8 = rdd7.flatMap(x => x._1.map { y => (y, x._2) })
  	val result = rdd8.reduceByKey(_ + _)
  	val result2 = result.map(x => (x._1, 0.15 + (0.85 * (x._2))))
  	val result3 = rdd3.leftOuterJoin(result2).mapValues{
		case (old, Some(n)) => n
		case (old, None) =>  0.15
	}

	result3.collect.foreach(x => println(">>>>>>>>>>>>>> " + x))  




  }
}
