import org.apache.spark._
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.fasterxml.jackson.databind.ObjectMapper

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

	val input = sc.textFile("/home/islam/spark-training-master/examples/formats/input/data.csv")
  	val result = input.map { x => 
    val reader = new CSVReader(new StringReader(x))
    	reader.readNext()
  	}
   
   val mapper = new ObjectMapper();
   result.map(mapper.writeValueAsString(_)).saveAsTextFile("output")
  
}  
  }
}
