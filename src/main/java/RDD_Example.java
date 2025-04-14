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

        //parallelize(list1) → tells Spark: "Hey, take this list and break it into parts so Spark can work on it in parallel."
        List<Integer> list1 = Arrays.asList(1,2,3,4);
        JavaRDD<Integer> rdd1 = sc.parallelize(list1);

        // map() : to perform some action on each element
        JavaRDD<Integer> mappedList= rdd1.map(x -> x*2);

        //filter() : Filters elements based on a condition.
        JavaRDD<Integer> filteredEvenElement = rdd1.filter(x -> x%2==0);

        //take(n) : Returns the first n elements.
        List<Integer> first2 = rdd1.take(2);

        //Print all results
        mappedList.collect().forEach(System.out::println);
        filteredEvenElement.collect().forEach(System.out::println);
        System.out.println(first2);

        // Step 4: Close Spark Context
        sc.close();
    }
}
