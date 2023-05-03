package fun.madeby.rdd;

// single column split into two columns key:value
// difference from MAP is that keys are being repeated, multi same key.
// BAG or MultiSET structure.
// Google Guava has BAG.
// PairRDD == extra methods:
// Fluent == returns value, no need for intermediate variables
// 

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class _06PairRDDs_Fluent {
public static void main(String[] args) {
      Logger.getRootLogger().setLevel(Level.WARN);

      List<String> input = new ArrayList<>();
      input.add("WARN: Tuesday 4 September 0405");
      input.add("ERROR: Tuesday 4 September 0408");
      input.add("FATAL: Wednesday 5 September 1632");
      input.add("WARN: Saturday 8 September 1942");

      SparkConf conf = new SparkConf().setAppName("startSpark2").setMaster("local[*]");
      JavaSparkContext sc = new JavaSparkContext(conf);

      // had issue with external processor earlier, had to add .collect();
      sc.parallelize(input)
        .mapToPair(val -> new Tuple2<>(val.split(":")[0], 1L))
          .reduceByKey(Long::sum)
          //.collect()
          .foreach(tuple -> System.out.println(tuple._1 +
              " @ " + tuple._2));

      sc.close();

}
}
