package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Collaborators {
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Collaborators");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println(ANSI_GREEN + "START" + ANSI_RESET);
        // Movie [(Actor, Actor)]
        // JavaPairRDD<String, Set<Tuple2<String, String>>>
        JavaPairRDD<String, Set<String>> result = sc.textFile("hdfs://namenode:9000/input/title.principals.tsv.bz2")
                .map(l -> l.split("\t+"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> l[3].contains("actor") || l[3].contains("actress") || l[3].contains("self"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(p._1,
                        StreamSupport.stream(p._2.spliterator(),false)
                                .flatMap(str1 -> StreamSupport.stream(p._2.spliterator(),false)
                                        .map(str2 -> new Tuple2<>(str1, str2)))
                                .collect(Collectors.toSet())
                ))
                .map(p -> p._2)
                .flatMap(Set::iterator)
                .mapToPair(p -> p)
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(p._1,
                        StreamSupport.stream(p._2.spliterator(),false)
                                .filter(l -> !p._1.equals(l))
                                .collect(Collectors.toSet())
                ))
                .cache();

        List<Tuple2<String, Set<String>>> actors = result.collect();
        System.out.println(ANSI_GREEN + actors.size() +  ANSI_RESET);

        for(int i = 0; i < actors.size(); i++)
            System.out.println(ANSI_GREEN + actors.get(i).toString() +  ANSI_RESET);

        System.out.println(ANSI_GREEN + "END" + ANSI_RESET);

    }
}
