package fun.madeby.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class _07FlatMaps {
public static void main(String[] args) {
      Logger.getRootLogger().setLevel(Level.WARN);

      List<String> input = new ArrayList<>();
      // AIM: Split these by word
      input.add("WARN: Tuesday 4 September 0405");
      input.add("");
      input.add("ERROR: Tuesday 4 September 0408");
      input.add("FATAL: Wednesday 5 September 1632");
      input.add("WARN: Saturday 8 September 1942");

      SparkConf conf = new SparkConf().setAppName("startSpark2").setMaster(
          "local[*]");

      try( JavaSparkContext sc = new JavaSparkContext(conf) ) {
//create string RDD (execution plan) from parallelized input
            JavaRDD<String> sentences = sc.parallelize(input);

//normal map syntax fails:
            //sentences.flatMap(value -> value.split(" "));
            sentences.flatMap(value -> Arrays.asList(value.split(
                         " ")).iterator())
                     .collect().forEach(System.out::println);

      }
}
}
