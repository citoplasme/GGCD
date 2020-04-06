package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Demo {
    public static void main(String[] args) throws Exception{
        int numMappers = 10;
        int numKVPairs = 100;
        int valSIze = 10;
        int numReducers = 5;

        SparkConf conf = new SparkConf().setAppName("GroupBy Test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = IntStream.range(0, numMappers).boxed().collect(Collectors.toList());
        JavaPairRDD<Integer, byte[]> pairs1 = sc.parallelize(data, numMappers)
                .flatMapToPair(p -> {
                    Random ranGen = new Random();
                    Stream<Tuple2<Integer, byte[]>> arr1 = IntStream.range(0, numKVPairs).mapToObj(i -> {
                        byte[] byteArr = new byte[valSIze];
                        ranGen.nextBytes(byteArr);
                        return new Tuple2<>(ranGen.nextInt(), byteArr);
                    });
                    return arr1.iterator();
                }).cache();
        long count1 = pairs1.count();
        long count2 = pairs1.groupByKey(numReducers).count();

        System.out.println(count1 + " " + count2);
    }
}
