import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple3;

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

        // Examples
        JavaRDD<Integer> myRdd1 = sc.parallelize(Arrays.asList(1,5,5,3,2,6));
        JavaRDD<Integer> myRdd2 = sc.parallelize(Arrays.asList(2,6,9,11));

        //union() : Combines two RDDs.
        JavaRDD<Integer> union = myRdd1.union(myRdd2);

        //intersection(): Returns common elements.
        JavaRDD<Integer> intersection = myRdd1.intersection(myRdd2);

        //distinct() : Removes duplicate elements.
        JavaRDD<Integer> distinct = myRdd1.distinct();

        //Print all results
        System.out.println("Examples of union() , intersection() & distinct()");

        union.collect().forEach(System.out::println);
        intersection.collect().forEach(System.out::println);
        distinct.collect().forEach(System.out::print);

        // Examples of Tuple
        // Tuple1 (holds 1 value)
        Tuple1<String> tuple1 = new Tuple1<>("Apple");
        System.out.println("Tuple 1 Value :" + tuple1._1);

        // Tuple2 (holds 2 value)
        Tuple2<String ,Integer> tuple2 = new Tuple2<>("Apple" ,100);
        System.out.println("Tuple 2 Values, Name :" + tuple2._1 +", Price : "+tuple2._2);

        // Tuple3 (holds 3 value)
        Tuple3<String, Integer , String> tuple3 = new Tuple3<>("Apple", 150 , "Sweet");
        System.out.println("Tuple 3 Value , Name :" + tuple3._1() +", Price : "+tuple3._2()+", Taste : "+tuple3._3());

        // PairRDD Example
        List<Tuple2<String,Integer>> rddPairData = Arrays.asList(
                new Tuple2<>("Mango",130),
                new Tuple2<>("Banana", 60),
                new Tuple2<>("Papaya", 45),
                new Tuple2<>("Pine Apple", 90),
                new Tuple2<>("Mango",150),
                new Tuple2<>("Banana", 40)
        );
        JavaPairRDD<String,Integer> fruitsPairRDD = sc.parallelizePairs(rddPairData);

        fruitsPairRDD.collect().forEach(System.out::println);

        // groupByKey() method
        JavaPairRDD <String , Iterable<Integer>> grouped = fruitsPairRDD.groupByKey();
        grouped.collect().forEach(System.out::println);

        // sortByKey()
        JavaPairRDD<String,Integer> sortedFruits = fruitsPairRDD.sortByKey();
        sortedFruits.collect().forEach(System.out::println);

        JavaRDD<String> listFromFile = sc.textFile("C:\\Users\\parmendra\\Desktop\\fruitsList.txt");
        System.out.println("Reading Data fromm file");
        listFromFile.collect().forEach(System.out::println);

        // Step 4: Close Spark Context
        sc.close();
    }
}
