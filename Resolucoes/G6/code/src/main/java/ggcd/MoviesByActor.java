package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class MoviesByActor {
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Movies By Actor");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println(ANSI_GREEN + "START" + ANSI_RESET);
        // Actor Number_of_Movies
        JavaPairRDD<String, Integer> movies_by_actor = sc.textFile("hdfs://namenode:9000/input/title.principals.tsv.bz2")
                .map(l -> l.split("\t+"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> l[3].contains("actor") || l[3].contains("actress") || l[3].contains("self"))
                .mapToPair(l -> new Tuple2<>(l[2], 1))
                .foldByKey(0, Integer::sum)
                .mapToPair(p -> new Tuple2<>(p._2, p._1))
                .sortByKey(false)
                .mapToPair(p -> new Tuple2<>(p._2, p._1))
                .cache();

        // Movies for each actor
        List<Tuple2<String, Integer>> actors = movies_by_actor.collect();
        System.out.println(ANSI_GREEN + actors.size() +  ANSI_RESET);

        // Top 10 Actors with most movies
        List<Tuple2<String, Integer>> top10 = movies_by_actor.take(10);

        for(Tuple2<String, Integer> actor : top10)
            System.out.println(ANSI_GREEN + actor._1 + " " + actor._2 +  ANSI_RESET);
        System.out.println(ANSI_GREEN + "END" + ANSI_RESET);

    }
}
