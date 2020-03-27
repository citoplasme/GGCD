package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Ex3 {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("g0spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Float> mr = sc.textFile("file:///Users/JoaoPimentel/desktop/4ANO/GGCD/Dados/data.tsv.gz")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], Float.parseFloat(l[1])))
                .foldByKey((float) 0, (v1, v2) -> v1 + v2); // In case we have duplicates
        List<Tuple2<String, Float>> ratings = mr.collect();

        System.out.println(ratings.toString());
    }
}
