package fun.madeby.utilities;
/**
 *  * to VM options via 'modify options':
 *  * --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 */

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Temp {
public static void main(String[] args) {
      Logger.getRootLogger().setLevel(Level.WARN);

      List<String> input = new ArrayList<>();
      input.add("WARN: Tuesday 4 September 0405");
      input.add("ERROR: Tuesday 4 September 0408");
      input.add("FATAL: Wednesday 5 September 1632");
      input.add("WARN: Saturday 8 September 1942");

      SparkConf conf = new SparkConf().setAppName("startSpark2").setMaster("local[*]");
      JavaSparkContext sc = new JavaSparkContext(conf);


      sc.close();

}
}
