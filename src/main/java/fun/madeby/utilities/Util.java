package fun.madeby.utilities;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class Util {

      private static Set<String> borings = new HashSet<>();
      // ADD SCALA and XML explicitly:
// https://stackoverflow/questions/41887530/
       static{
            InputStream is =
                     Util.class.getResourceAsStream("/subtitles/boringwords" +
                         ".txt");
                  BufferedReader br =
                      new BufferedReader(new InputStreamReader(is));
                  br.lines().forEach(it -> borings.add(it));
      }


      public static boolean isBoring(String word) {
            return borings.contains(word);
      }

      public static boolean isNotBoring(String word) {
            return !isBoring(word);
      }
}
