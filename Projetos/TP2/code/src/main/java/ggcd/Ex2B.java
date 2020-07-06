package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

// Friends: Calcule o conjunto de colaboradores de cada ator (i.e., outros atores que participaram nos mesmos títulos).
public class Ex2B {
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Friends");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println(ANSI_GREEN + "START" + ANSI_RESET);
        double start = System.currentTimeMillis();

        // Filme Ator
        JavaPairRDD<String, String> pares = sc.textFile("hdfs://namenode:9000/input/title.principals.tsv")
            .map(l -> l.split("\t+"))
            .filter(l -> !l[0].equals("tconst"))
            .filter(l -> l[3].contains("actor") || l[3].contains("actress") || l[3].contains("self"))
            .mapToPair(l -> new Tuple2<>(l[0], l[2]))
            .cache();
            //.persist(StorageLevel.DISK_ONLY());

        // Ator [Ator]
        //JavaPairRDD<String, Iterable<String>> result = pares.join(pares)
        //JavaPairRDD<String, String> result = pares.join(pares)
        pares.join(pares)
            .filter(p -> !p._2._1.equals(p._2._2))
            .mapToPair(p -> p._2)
            .groupByKey()
            //.cache();
            //.coalesce(1)
            .saveAsTextFile("hdfs://namenode:9000/output/collaborators/");

        //List<Tuple2<String, Iterable<String>>> actors = result.collect();
        /*
        List<Tuple2<String, String>> actors = result.collect();
        System.out.println(ANSI_GREEN + actors.size() +  ANSI_RESET);

        //for (Tuple2<String, Iterable<String>> actor : actors)
        for (Tuple2<String, String> actor : actors)
            System.out.println(ANSI_GREEN + actor.toString() + ANSI_RESET);
        */
        double end = System.currentTimeMillis();
        System.out.println("Time to process: " + (end - start) + "ms");
        System.out.println(ANSI_GREEN + "END" + ANSI_RESET);

    }
}
