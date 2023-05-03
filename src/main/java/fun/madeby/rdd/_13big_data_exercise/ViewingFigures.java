package fun.madeby.rdd._13big_data_exercise;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ViewingFigures {
  public static void main(String[] args) {
    Logger.getRootLogger().setLevel(Level.WARN);
    System.setProperty("hadoop.home.dir", "c:/hadoop");

    SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Use true to use hardcoded data identical to that in the PDF guide.
    boolean testMode = false;

    JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
    JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
    JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

    if (testMode) {
      System.out.println("TEST DATA LOADED");
      System.out.println("viewData userID | chapterID");
      viewData.collect().forEach(System.out::println);
      System.out.println("chapterData  chapterID | courseID");
      chapterData.collect().forEach(System.out::println);
      System.out.println("titlesData courseID | courseTitle");
      titlesData.collect().forEach(System.out::println);
      System.out.println("========================END======================\n\n");
    }

    // Warmup
    // A switch chapter id and course id in chapter data:
    System.out.println("WARM UP, count chapter views by user ID");
    JavaPairRDD<Integer, Integer> cd_switchCounted =
        chapterData
            .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)) // course ID <-
            .mapToPair(tuple -> new Tuple2<>(tuple._1, 1)) // chapter to 1
            .reduceByKey(Integer::sum); // count when val 1
    System.out.println("orig chapter data");
    chapterData.collect().forEach(System.out::println);
    System.out.println("switched and counted chapterData");
    cd_switchCounted.collect().forEach(System.out::println);

    // Step 1 - remove any duplicated views that do not count
    System.out.println("Step 1: remove dupe views: viewData.distinct()");
    JavaPairRDD<Integer, Integer> vd_distinct = viewData.distinct();
    vd_distinct.collect().forEach(System.out::println);

    // Step 2 - get the course Ids into the RDD ChapterID is common key and has already been <-
    // switched
    System.out.println("STEP2 - get course IDs into the RDD");
    System.out.println("original chapterData");
    chapterData.collect().forEach(System.out::println);
    System.out.println("distinct viewData userID | chapID");
    vd_distinct.collect().forEach(System.out::println);
    JavaPairRDD<Integer, Integer> vd_switched = vd_distinct.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
    System.out.println("switched viewData chapID | userID");
    vd_switched.collect().forEach(System.out::println);

    JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinReduce =  vd_switched.join(chapterData);
    System.out.println("vd_switched.join(chapterData)");
    joinReduce.collect().forEach(System.out::println);

    // Step 3 - don't need chapterIds, setting up for a reduce
    System.out.println("STEP 3, no longer need chapter IDs");
    JavaPairRDD<Integer, Integer> userViewsOfCourse = joinReduce.mapToPair(tuple -> new Tuple2<>(tuple._2._1, tuple._2._2));
    userViewsOfCourse.collect().forEach(System.out::println);
    System.out.println("STEP 3a, prepare for count, drop in an int");
    JavaPairRDD<Tuple2<Integer, Integer>, Integer>  courseViewsCountReady = userViewsOfCourse
        .mapToPair(tuple -> new Tuple2<>(tuple, 1));
    courseViewsCountReady.collect().forEach(System.out::println);

    // Step 4 - count how many views for each user per course
    System.out.println("STEP 4, count views for each user per course");
    JavaPairRDD<Tuple2<Integer, Integer>, Integer> courseViewsCount = courseViewsCountReady.reduceByKey(Integer::sum);
    courseViewsCount.collect().forEach(System.out::println);


    // step 5 - remove the userIds
    System.out.println("STEP 5, remove, no longer needed userIDs");
    JavaPairRDD<Integer, Integer> countByCourse = courseViewsCount.mapToPair(tuple -> new Tuple2<>(tuple._1._2, tuple._2 ));
    countByCourse.collect().forEach(System.out::println);

    // step 6 - add in the total chapter count
    System.out.println("STEP 6, add the total chapter count so: courseID|views|of");
    System.out.println("chapter data switched and counted: ");
    cd_switchCounted.collect().forEach(System.out::println);
    System.out.println("(ID(views,of))------------------------");
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> viewsOfByCourse = countByCourse.join(cd_switchCounted);
    viewsOfByCourse.collect().forEach(System.out::println);

    // step 7 - convert to percentage
    System.out.println("STEP 7, convert to a percentage");
    JavaPairRDD<Integer, Double> percentage = viewsOfByCourse.mapToPair(tuple -> new Tuple2<>(tuple._1,(double)tuple._2._1 / (double)tuple._2._2));
    percentage.collect().forEach(System.out::println);

    // step 8 - convert to scores
    System.out.println("STEP 8, percentages are converted to scores");
    JavaPairRDD<Integer, Integer> scores = percentage.mapToPair(tuple -> new Tuple2<>(tuple._1,
        tuple._2 > 0.25 ? (tuple._2 > 0.5 ? tuple._2 > 0.9 ? 10:4 :2):0
        ));
    scores.collect().forEach(System.out::println);

    // step 9 tally scores
    System.out.println("STEP 9, tally scores");
    JavaPairRDD<Integer, Integer>  talliedScores =
    scores.reduceByKey((course, score) -> course + score);
    talliedScores.collect().forEach(System.out::println);

    // step 10
    System.out.println("STEP 10, add course names and sort descending");


    List<Tuple2<Integer, Tuple2<String, Integer>>> finalList =
    talliedScores
        .join(titlesData)
        .mapToPair(tuple -> new Tuple2<>(tuple._2._1, new Tuple2<>(tuple._2._2, tuple._1)))
        .sortByKey(false).collect();

    for( Tuple2<Integer, Tuple2<String, Integer>> tup : finalList ) {
      System.out.println(tup._1 + " Points scored by course: " + tup._2._1 + " course num: " + tup._2._2);
    }



    sc.close();
  }

  private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(
      JavaSparkContext sc, boolean testMode) {

    if (testMode) {
      // (chapterId, title)
      List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
      rawTitles.add(new Tuple2<>(1, "How to find a better job"));
      rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
      rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
      return sc.parallelizePairs(rawTitles);
    }
    return sc.textFile("src/main/resources/viewing figures/titles.csv")
        .mapToPair(
            commaSeparatedLine -> {
              String[] cols = commaSeparatedLine.split(",");
              return new Tuple2<>( Integer.parseInt(cols[0]), cols[1]);
            });
  }

  private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(
      JavaSparkContext sc, boolean testMode) {

    if (testMode) {
      // (chapterId, (courseId, courseTitle))
      List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
      rawChapterData.add(new Tuple2<>(96, 1));
      rawChapterData.add(new Tuple2<>(97, 1));
      rawChapterData.add(new Tuple2<>(98, 1));
      rawChapterData.add(new Tuple2<>(99, 2));
      rawChapterData.add(new Tuple2<>(100, 3));
      rawChapterData.add(new Tuple2<>(101, 3));
      rawChapterData.add(new Tuple2<>(102, 3));
      rawChapterData.add(new Tuple2<>(103, 3));
      rawChapterData.add(new Tuple2<>(104, 3));
      rawChapterData.add(new Tuple2<>(105, 3));
      rawChapterData.add(new Tuple2<>(106, 3));
      rawChapterData.add(new Tuple2<>(107, 3));
      rawChapterData.add(new Tuple2<>(108, 3));
      rawChapterData.add(new Tuple2<>(109, 3));
      return sc.parallelizePairs(rawChapterData);
    }

    return sc.textFile("src/main/resources/viewing figures/chapters.csv")
        .mapToPair(
            commaSeparatedLine -> {
              String[] cols = commaSeparatedLine.split(",");
              return new Tuple2<>(Integer.parseInt(cols[0]), Integer.parseInt(cols[1]));
            });
  }

  private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(
      JavaSparkContext sc, boolean testMode) {

    if (testMode) {
      // Chapter views - (userId, chapterId)
      List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
      rawViewData.add(new Tuple2<>(14, 96));
      rawViewData.add(new Tuple2<>(14, 97));
      rawViewData.add(new Tuple2<>(13, 96));
      rawViewData.add(new Tuple2<>(13, 96));
      rawViewData.add(new Tuple2<>(13, 96));
      rawViewData.add(new Tuple2<>(14, 99));
      rawViewData.add(new Tuple2<>(13, 100));
      return sc.parallelizePairs(rawViewData);
    }

    return sc.textFile("src/main/resources/viewing figures/views-*.csv")
        .mapToPair(
            commaSeparatedLine -> {
              String[] columns = commaSeparatedLine.split(","); // below was new Integer(col[x])
              return new Tuple2<>(
                  Integer.parseInt(columns[0]), Integer.parseInt(columns[1]));
            });
  }

private static Tuple2<Integer, Integer> call(String commaSeparatedLine) {
  String[] cols = commaSeparatedLine.split(",");
  return new Tuple2<>(Integer.parseInt(cols[0]),  Integer.parseInt(cols[1]));
}
}
