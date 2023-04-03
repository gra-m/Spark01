package fun.madeby;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class _05Tuples {
public static void main(String[] args) {
      //Logger.getRoot().setLevel(Level.toLevel("WARN"));
      Logger.getRootLogger().setLevel(Level.WARN);

      List<Integer> input = new ArrayList<>();
      input.add(10);
      input.add(11);
      input.add(14);
      input.add(25);

      SparkConf conf = new SparkConf().setAppName("startSpark2").setMaster(
          "local[*]");
      JavaSparkContext sc = new JavaSparkContext(conf);

      /**
       * Pair up data without TUPLE
       */
      JavaRDD<Integer> origInts = sc.parallelize(input);
      //JavaRDD<IntWithSqR> sqrt = origInts.map(a ->new IntWithSqR(a));
      JavaRDD<IntWithSqR> withSqRt = origInts.map(IntWithSqR::new);

      /**
       * Pair up data with TUPLES no need for class
       */
      Tuple2<Integer, Double> myValue = new Tuple2<>(9,3.0);
      
      JavaRDD<Tuple2<Integer,Double>> withSqRt2 =
          origInts.map(a -> new Tuple2<>(a, Math.sqrt(a)));
      
      withSqRt2.collect().forEach(a -> System.out.println(a));
      withSqRt2.collect().forEach(System.out::println);




      sc.close();


}

private static class IntWithSqR {
      private int origNum;
      private double sqrt;
      
      public IntWithSqR(int i)
      {
            this.origNum = i;
            this.sqrt = Math.sqrt(i);
      }
}
}
