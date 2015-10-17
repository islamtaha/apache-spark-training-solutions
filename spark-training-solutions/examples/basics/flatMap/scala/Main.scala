import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq("a b", "b c", "c d", "d e"))

    val output = rdd.flatMap(x => x.split(" "))

    output.collect.foreach(x => println(">>>>>>>>>>>>> " + x ))
  
  }
}
