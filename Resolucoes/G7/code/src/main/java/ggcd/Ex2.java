package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.StreamSupport;

// Compute every minute the top 3 highest ranked movie identifiers in the last 10 minutes.
public class Ex2 {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("persstream");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));
        sc.socketTextStream("localhost", 12345)
                .window(Durations.minutes(10),Durations.seconds(60))
                .map(t -> t.split("\t+"))
                .mapToPair(t -> new Tuple2<>(t[0],Double.parseDouble(t[1])))
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(
                        StreamSupport.stream(p._2.spliterator(),false)
                                .mapToDouble(a -> a)
                                .average()
                                .getAsDouble(), p._1
                ))
                .foreachRDD(rdd -> {
                    List<Tuple2<Double, String>> lst = rdd.sortByKey(false).take(3);
                    System.out.println("TOP 3: " + lst.toString());
                });

        sc.start();
        sc.awaitTermination();
    }
}
