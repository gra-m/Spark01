package fun.madeby.rdd;

/*
 * Add this:
 *       https://i.stack.imgur.com/JYD4C.png
 * to VM options via 'modify options':
 * --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 * */

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class _04Main {
public static void main(String[] args) {
      // set log level on spark context after created NONE WORKED after rc!!:
      //Logger.getRootLogger().setLevel(Level.INFO);
      //Logger.getLogger("org.apache").setLevel(Level.WARN);
      // NO Logger.getLogger(_01Main.class).setLevel(Level.OFF);
      // NO Logger.getLogger("org.apache").setLevel(Level.WARN);
      //Logger.getRoot().setLevel(Level.toLevel("INFO"));
      Logger.getRootLogger().setLevel(Level.WARN);
      

      List<Double> inputData = new ArrayList<>();
      inputData.add(35.5);
      inputData.add(12.49456);
      inputData.add(90.32);
      inputData.add(20.54);



      SparkConf conf = new SparkConf().setAppName("spark01").setMaster("local"
          + "[*]");
      JavaSparkContext sc = new JavaSparkContext(conf);





      // wrapper for Scala RDD -> adds RDD to Spark's execution plan
      JavaRDD<Double> myRdd;
      JavaRDD<Double> sqrtRdd;

      myRdd = sc.parallelize(inputData);
      sqrtRdd = sc.parallelize(inputData);


      //REDUCE:
      //myRdd.reduce( (value1, value2) -> value1 + value2 );
      System.out.println("001 --> " + myRdd.reduce(Double::sum));

      //MAP:
      //myRdd.map(a -> Math.sqrt(a));
      JavaRDD<Double> result = sqrtRdd.map(Math::sqrt);
      System.out.println("002 --> does not print " + result +
              "\nSo, print out with sqrtRdd.!!collect().forEach(System" +
          ".out::println):\n");


      //sqrtRdd.foreach(value -> System.out.println(value));

      try {
            //result.collect().forEach(value -> System.out.println(value));
            result
                .collect()
                .forEach(System.out::println);}
      catch (IllegalAccessError e) {
            System.out.println("This error: because module java.base does not " +
          "export sun.security.action to unnamed module @0x6253c26 was caused" +
                " where multiple cpus are in use, like passing any function " +
                    "to a cloud node it is serialized first and so fails if " +
                    "not first 'collected'");
            e.printStackTrace();
      }

      //COUNT
      System.out.println("\nJava answer on count == sqrtRdd.count()" + sqrtRdd.count());

      JavaRDD<Long> countRdd = sqrtRdd.map(a -> 1L);
      //countRdd.reduce((a,b) -> a + b);
      countRdd.reduce(Long::sum);

      System.out.println("\nMid flow, useable in query answer for RDD count " +
          "== sqrtRdd.map(a -> 1).reduce(Long::sum); ------> " + countRdd.reduce(Long::sum));

      //sqrtRdd.map(a->1).reduce(Integer::sum);


      // not strictly necessary for spark.
      sc.close();

}
}
