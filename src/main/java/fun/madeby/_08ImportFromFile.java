package fun.madeby;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class _08ImportFromFile {
public static void main(String[] args) {
      Logger.getRootLogger().setLevel(Level.WARN);


      SparkConf conf = new SparkConf().setAppName("startSpark2").setMaster(
          "local[*]");

      try( JavaSparkContext sc = new JavaSparkContext(conf) ) {


            JavaRDD<String> initialRdd = sc.textFile("src/main/resources" +
                "/subtitles/input.txt");

            initialRdd
                .flatMap(value -> Arrays.asList(value.split(" "))
                                        .iterator())
                .take(10)
                //.collect()
                .forEach(System.out::println);

      }
}
}
