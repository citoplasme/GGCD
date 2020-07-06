package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.List;

// Ratings: Atualize o ficheiro title.ratings.tsv tendo em conta o seu conteúdo anterior e os novos votos
// recebidos até ao momento.
public class Ex2C {
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Update Ratings");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println(ANSI_GREEN + "START" + ANSI_RESET);
        // Movie (Score, Number of Votes)
        JavaPairRDD<String, Tuple2<Double, Integer>> movies_by_actor = sc.textFile("hdfs://namenode:9000/input/title.ratings.tsv")
                .map(l -> l.split("\t+"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<Double, Integer>(Double.parseDouble(l[1]), Integer.parseInt(l[2]))))
                .cache();

        // Movie (Score, Number of Votes)
        JavaPairRDD<String, Tuple2<Double, Integer>> logs = sc.textFile("hdfs://namenode:9000/logs/*")
                .map(l -> l.split("\t+"))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Double.parseDouble(l[1]), 1)))
                .reduceByKey(
                        (Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>)
                                (i1, i2) -> new Tuple2<>(i1._1 + i2._1, i1._2 + i2._2))
                .mapToPair(v -> new Tuple2<>(v._1, new Tuple2<>(v._2._1 / v._2._2, v._2._2)))
                .cache();

        // Merge e cálculo da nova média e novo total de votos
        //JavaPairRDD<String, Tuple2<Double, Integer>> result = movies_by_actor
        movies_by_actor.join(logs)
            .mapToPair(p -> new Tuple2<>(p._1, new Tuple2<>((p._2._1._1 * p._2._1._2 + p._2._2._1 * p._2._2._2) / (p._2._1._2 + p._2._2._2), p._2._1._2 + p._2._2._2)))
            .map(p -> p._1 + "\t" + p._2._1 + "\t" + p._2._2)
            //.coalesce(1)
            .saveAsTextFile("hdfs://namenode:9000/output/title_ratings_new/");
            //.cache();

        // Movies for each actor
        /*
        List<Tuple2<String, Tuple2<Double, Integer>>> res = result.collect();
        System.out.println(ANSI_GREEN + res.size() +  ANSI_RESET);
        for(Tuple2<String, Tuple2<Double, Integer>> movie : res)
            System.out.println(ANSI_GREEN + movie._1 + " " + movie._2 +  ANSI_RESET);

        result.coalesce(1).saveAsTextFile("hdfs://namenode:9000/output/title_ratings_new");
        */
        System.out.println(ANSI_GREEN + "END" + ANSI_RESET);

    }
}
