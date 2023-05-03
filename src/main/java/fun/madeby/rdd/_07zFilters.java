package fun.madeby.rdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class _07zFilters {
  public static void main(String[] args) {
    Logger.getRootLogger().setLevel(Level.WARN);

    List<String> input = new ArrayList<>();

    input.add("WARN: Tuesday 4 September 0405");
    input.add("xerxes");
    input.add("ERROR: Tuesday 4 September 0408");
    input.add("FATAL: Wednesday 5 September 1632");
    input.add("WARN: Saturday 8 September 1942");

    SparkConf conf = new SparkConf().setAppName("startSpark2").setMaster("local[*]");

    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      // create string RDD (execution plan) from parallelized input
      JavaRDD<String> sentences = sc.parallelize(input);

      //STEP 1
      JavaRDD<String> elements =
          sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

      // Remove single characters contract false filter out true retain:
      elements.filter(el -> el.length() > 5).collect().forEach(System.out::println);
    }
  }
}
