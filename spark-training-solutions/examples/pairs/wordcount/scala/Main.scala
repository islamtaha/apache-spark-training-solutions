import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/home/islam/spark-training-master/examples/pairs/wordcount/input")

    val words = rdd.flatMap(x => x.split(" ")).map(_ -> 1)
    val wordcount = words.reduceByKey(_ + _)

    wordcount.saveAsTextFile("output")

  }
}
