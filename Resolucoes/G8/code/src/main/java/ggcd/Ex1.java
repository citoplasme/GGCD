package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Ex1 {
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Movies By Actor");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println(ANSI_GREEN + "START" + ANSI_RESET);
        double start = System.currentTimeMillis();
        // Movie Actor
        JavaPairRDD<String, String> movies_by_actor = sc.textFile("hdfs://namenode:9000/input/title.principals.tsv")
                .map(l -> l.split("\t+"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> l[3].contains("actor") || l[3].contains("actress") || l[3].contains("self"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                .cache();

        // Movie Rating
        JavaPairRDD<String, Float> ratings = sc.textFile("hdfs://namenode:9000/input/title.ratings.tsv")
                .map(l -> l.split("\t+"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], Float.parseFloat(l[1])))
                .cache();

        // Actor [(Movie, Rating)]
        JavaPairRDD<String, ArrayList<Tuple2<String, Float>>> result = movies_by_actor.join(ratings)
                .mapToPair(p -> new Tuple2<>(p._2._1, new Tuple2<>(p._1, p._2._2)))
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(p._1,
                        StreamSupport.stream(p._2.spliterator(),false)
                                .sorted((a, b) -> b._2.compareTo(a._2))
                                .collect(Collectors.toList())
                ))
                .mapToPair(p -> new Tuple2<>(p._1, new ArrayList<>(p._2.subList(0, Math.min(p._2.size(), 3)))))
                .cache();

        List<Tuple2<String, ArrayList<Tuple2<String, Float>>>> actors = result.collect();
        double end = System.currentTimeMillis();
        System.out.println(ANSI_GREEN + actors.size() +  ANSI_RESET);

        for(int i = 0; i < actors.size(); i++)
            System.out.println(ANSI_GREEN + actors.get(i).toString() +  ANSI_RESET);
        System.out.println("Time to process: " + (end - start) + "ms");
        System.out.println(ANSI_GREEN + "END" + ANSI_RESET);

    }
}
