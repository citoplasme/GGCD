package ggcd;

import org.apache.commons.lang.math.FloatRange;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Ex3 {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("g0spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Ratings
        JavaPairRDD<String, Float> ratings = sc.textFile("file:///Users/JoaoPimentel/desktop/4ANO/GGCD/Dados/data.tsv.gz")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], Float.parseFloat(l[1])));
        // Titles
        JavaPairRDD<String, String> titles = sc.textFile("file:///Users/JoaoPimentel/desktop/4ANO/GGCD/Dados/data-5.tsv.gz")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));
        // Merge
        List<Tuple2<String,Tuple2<Float,String>>> merge = ratings.join(titles).collect();

        List<Tuple2<String,Float>> result = merge.stream()
                .map(t -> new Tuple2<>(t._2._2,t._2._1)).collect(Collectors.toList());

        System.out.println(result.toString());
    }
}
