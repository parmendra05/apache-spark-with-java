import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDD_Example {
    public static void main(String[] args) {
        // Step 1: Create Spark Configuration & Context
        SparkConf conf = new SparkConf().setAppName("RDD Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Step 2: Create an RDD from a List
        List<String> data = Arrays.asList("Java", "Spring Boot", "Apache Spark", "Big Data");
        JavaRDD<String> rdd = sc.parallelize(data);

        // Step 3: Print RDD elements
        rdd.collect().forEach(System.out::println);

        // count() method
        System.out.println(rdd.count());

        // map() : to perform some action on each element
        JavaRDD<String> upperCaseData=rdd.map(String::toUpperCase);
        upperCaseData.collect().forEach(System.out::println);

        // Step 4: Close Spark Context
        sc.close();
    }
}
