import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val numbers1 = sc.parallelize(Seq(1, 2, 3, 4))

  	val numbers2 = sc.parallelize(Seq(2, 4, 6, 8))

    val numbers = numbers1.intersection(numbers2)

    numbers.collect.foreach(x => println(">>>>>>>>>> " + x))  
  
  	
  }
}
