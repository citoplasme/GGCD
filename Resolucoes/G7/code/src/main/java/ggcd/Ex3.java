package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;
import java.util.stream.StreamSupport;

// Compute every minute the top 3 highest ranked movie titles in the last 10 minutes.
public class Ex3 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("persstream");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));

        JavaPairRDD<String,String> movies = sc.sparkContext().textFile("/Users/JoaoPimentel/desktop/4ANO/GGCD/Dados/title.basics.tsv.gz")
                .map(t -> t.split("\t+"))
                .mapToPair(t -> new Tuple2<>(t[0],t[2]))
                .cache();

        sc.socketTextStream("localhost", 12345)
                .window(Durations.minutes(10),Durations.seconds(60))
                .map(t -> t.split("\t+"))
                .mapToPair(t -> new Tuple2<>(t[0],Double.parseDouble(t[1])))
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(p._1,
                        StreamSupport.stream(p._2.spliterator(),false)
                                .mapToDouble(a -> a)
                                .average()
                                .getAsDouble()
                ))
                .foreachRDD(rdd -> {
                    JavaPairRDD<Double,String> res = rdd
                            .mapToPair(t -> new Tuple2<>(t._1,t._2))
                            .join(movies)
                            .mapToPair(t -> new Tuple2<>(t._2._1,t._2._2))
                            .sortByKey(false);
                    List<Tuple2<Double, String>> lst = res.take(3);
                    System.out.println("TOP 3: " + lst.toString());
                });

        sc.start();
        sc.awaitTermination();
    }
}
