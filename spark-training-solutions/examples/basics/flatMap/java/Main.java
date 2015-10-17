import java.util.*;
import org.apache.spark.api.java.*;
import org.apache.spark.*;

public class Main {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRdd<String> rdd = sc.parallelize(Arrays.asList("a b", "b c", "c d", "d e"));

	JavaRdd<String> output = rdd.map(x -> x.split(" "));

	output.foreach(x -> System.out.println(">>>>>>>>>>>>>> " + x));

  }
}
