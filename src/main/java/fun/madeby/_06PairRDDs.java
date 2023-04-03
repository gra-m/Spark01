package fun.madeby;

// single column split into two columns key:value
// difference from MAP is that keys are being repeated, multi same key.
// BAG or MultiSET structure.
// Google Guava has BAG.
// PairRDD == extra methods:
// 

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class _06PairRDDs {
public static void main(String[] args) {
      Logger.getRootLogger().setLevel(Level.WARN);

      List<String> input = new ArrayList<>();
      input.add("WARN: Tuesday 4 September 0405");
      input.add("ERROR: Tuesday 4 September 0408");
      input.add("FATAL: Wednesday 5 September 1632");
      input.add("WARN: Saturday 8 September 1942");

      SparkConf conf = new SparkConf().setAppName("startSpark2").setMaster("local[*]");
      JavaSparkContext sc = new JavaSparkContext(conf);

      JavaRDD<String> origStrings = sc.parallelize(input);

      JavaPairRDD<String, String> pairRDD = origStrings.mapToPair(rawVal -> {
            String[] cols = rawVal.split(":");
            String level = cols[0];
            String date = cols[1];

            return new Tuple2<>(level, date);

      });

      //Group by key == poor performance <String, Iterable<String>
      //pairRDD.groupByKey();

      JavaPairRDD<String, Long> pairRDD2 = origStrings.mapToPair(a -> {
            String [] cols = a.split(":");
            String level = cols[0];
            return new Tuple2<>(level, 1L);
      });

      //pairRDD2.reduceByKey((a, b) -> a + b);
      JavaPairRDD<String, Long> result = pairRDD2.reduceByKey(Long::sum);

      result.collect().forEach(tuple -> System.out.println(tuple._1 + " " + tuple._2));


      sc.close();

}
}
