import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val input = ssc.socketTextStream("localhost", 7777)
    val output1 = input.map(_.split(" ")).map(x => x(0) -> x(1))
    

	
    val input2 = ssc.socketTextStream("localhost", 7776)
    val output2 = input.map(_.split(" ")).map(x => x(0) -> x(1))

	val output = output1.join(output2)
    output.print
	
    ssc.start()
    ssc.awaitTermination()
  }
}
