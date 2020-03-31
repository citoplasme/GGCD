package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Ex4 {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("g0spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Float> mr = sc.textFile("file:///Users/JoaoPimentel/desktop/4ANO/GGCD/Dados/data.tsv.gz")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> Float.parseFloat(l[1]) >= 9.0)
                .mapToPair(l -> new Tuple2<>(l[0], Float.parseFloat(l[1])));

        List<Tuple2<String, Float>> ratings = mr.collect();
        List<Tuple2<String, Float>> lista = new ArrayList<>(ratings);
        lista.sort((a, b) -> b._2.compareTo(a._2));

        System.out.println(lista.toString());
    }
}
