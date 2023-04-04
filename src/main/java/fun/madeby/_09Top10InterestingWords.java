package fun.madeby;


import fun.madeby.utilities.Util;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;



public class _09Top10InterestingWords {
public static void main(String[] args) {
      Logger.getRootLogger().setLevel(Level.WARN);


      SparkConf conf = new SparkConf().setAppName("startSpark2").setMaster(
          "local[*]");

      try( JavaSparkContext sc = new JavaSparkContext(conf) ) {


            JavaRDD<String> initialRdd = sc.textFile("src/main/resources" +
                "/subtitles/input.txt");

            initialRdd.map(line -> line.replaceAll("[^a-zA-Z\\s]", "")
                                       .toLowerCase())
                .filter(line -> line.trim().length() > 0)
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .filter(word -> word.trim().length() > 0)
                .filter(word -> Util.isNotBoring(word))
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((value, count) -> value + count)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(10)
                .forEach(System.out::println);
      }
}

private static class CheckAgainstList {
      private static Set<String> borings = new HashSet<>();

      static {
            try {
                  InputStream is =
                      CheckAgainstList.class.getResourceAsStream(
                          "/subtitles/boringwords.txt");
                  createBufferAndPopulate(is);
            } catch (ExceptionInInitializerError e) {
                  e.printStackTrace();
            }

      }

      private static void createBufferAndPopulate(InputStream is) {
            try {
                  BufferedReader br = new BufferedReader(new InputStreamReader(is));
                  br.lines().forEach(it -> borings.add(it));
                  if (borings.size() == 0)
                        throw new RuntimeException("List is empty");
            } catch (ExceptionInInitializerError e) {
                  e.printStackTrace();
            }
      }

      public static boolean isBoring(String word) {
            return borings.contains(word);
      }

      public static boolean isNotBoring(String word) {
            return !(isBoring(word));
      }

}
}
