package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.List;
import java.util.Optional;

// Save incoming data in text files every 1 minute.
public class Ex1 {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("persstream");
        /*
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("/tmp/stream", () -> {
            JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));
            sc.socketTextStream("localhost", 12345).map(String::toUpperCase).print();
            sc.checkpoint("/tmp/stream");
            return sc;
        });
         */
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));
        sc.socketTextStream("localhost", 12345)
                .foreachRDD(rdd -> {
                    rdd.saveAsTextFile("/Users/JoaoPimentel/desktop/4ANO/GGCD/Resolucoes/G7");
                });
        sc.start();
        sc.awaitTermination();
    }
}
