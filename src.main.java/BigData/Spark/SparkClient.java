package BigData.Spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SparkClient {

	public static void main(String[] args) {
	    String logFile = "C:/Program Files/Apache/spark-3.1.1-bin-hadoop3.2/README.md"; // Should be some file on your system
	    SparkSession spark = SparkSession.builder().config("spark.master", "local").appName("Simple Application").getOrCreate();
	    Dataset<String> logData = spark.read().textFile(logFile).cache();

	    long numAs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("a")).count();
	    long numBs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("b")).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

	    spark.stop();
	}
}
