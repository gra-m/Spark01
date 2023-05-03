package fun.madeby.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class _10TestJoinData {
public static void main(String[] args) {
      Logger.getRootLogger().setLevel(Level.WARN);

      SparkConf conf = new SparkConf()
          .setAppName("startSpark")
          .setMaster("local[*]");

      try (JavaSparkContext sc = new JavaSparkContext(conf)) {
           List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
           List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();

            visitsRaw.add(new Tuple2<>(4, 18));
            visitsRaw.add(new Tuple2<>(6, 4));
            visitsRaw.add(new Tuple2<>(10, 9));

            usersRaw.add(new Tuple2<>(1, "John"));
            usersRaw.add(new Tuple2<>(2, "Bob"));
            usersRaw.add(new Tuple2<>(3, "Alan"));
            usersRaw.add(new Tuple2<>(4, "Doris"));
            usersRaw.add(new Tuple2<>(5, "Marybelle"));
            usersRaw.add(new Tuple2<>(6, "Raquel"));

            JavaPairRDD<Integer, Integer> visits =
                sc.parallelizePairs(visitsRaw);
            JavaPairRDD<Integer, String> users =
                sc.parallelizePairs(usersRaw);

      System.out.println("----ORIG DATA----");
      System.out.println("Visits");
      visits.collect().forEach(System.out::println);
      System.out.println("Users");
      users.collect().forEach(System.out::println);
      System.out.println("---------------------\n\n");

            System.out.println("INNER");JavaPairRDD<Integer, Tuple2<Integer, String>> inner =  visits.join(users);
      inner.collect().forEach(System.out::println);


            System.out.println("LEFT_OUTER");
      JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> lOuter =
          visits.leftOuterJoin(users);

            lOuter.collect().forEach(System.out::println);

            System.out.println("RIGHT_OUTER");
            JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rOuter =
                visits.rightOuterJoin(users);


            rOuter.collect().forEach(System.out::println);

      // todo Optional.orElse("blank")
      System.out.println("LEFT_OUTER optional cleaned to 'blank', all to uppercase");
            lOuter.collect().forEach(
                it -> System.out.println(it._2._2.orElse("blank")
                                                 .toUpperCase()));


      }


}
}
