package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class MoviesByActor {
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Movies By Actor");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println(ANSI_GREEN + "START" + ANSI_RESET);
        // Actor Number_of_Movies
        JavaPairRDD<String, Integer> movies_by_actor = sc.textFile("hdfs://namenode:9000/input/title.principals.tsv.bz2")
                .map(l -> l.split("\t+"))
                .filter(l -> !l[0].equals("tconst"))
                //.filter(l -> l[3].contains("actor") || l[3].contains("actress") || l[3].contains("self"))
                .mapToPair(l -> new Tuple2<>(l[2], 1))
                .foldByKey(0, Integer::sum);
                //.cache();

        List<Tuple2<String, Integer>> movies = movies_by_actor.collect();

        System.out.println(ANSI_GREEN + movies.toString() + ANSI_RESET);

        System.out.println(ANSI_GREEN + "END" + ANSI_RESET);
    }
}
