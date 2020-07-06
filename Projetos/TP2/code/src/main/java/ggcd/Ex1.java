package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;

class ComparadorTuplos implements Comparator<Tuple2<String, Double>>, Serializable {
    final static ComparadorTuplos INSTANCE = new ComparadorTuplos();
    public int compare(Tuple2<String, Double> t1, Tuple2<String, Double> t2) {
        return -t1._2.compareTo(t2._2);
    }
}

// As 3 tarefas da alínea 1 devem ocorrer em simultâneo
public class Ex1 {
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("Streaming");

        int batch = 60;

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(batch));
        sc.checkpoint("hdfs://namenode:9000/checkpoints/");

        // Load dos dados presentes em ficheiro
        JavaPairRDD<String,String> movies = sc.sparkContext().textFile("hdfs://namenode:9000/input/title.basics.tsv")
            .map(t -> t.split("\t+"))
            .mapToPair(t -> new Tuple2<>(t[0],t[2]))
            .cache();

        JavaDStream<String> input = sc.socketTextStream("streamgen", 12345);
        // Alínea A
        // Log: Armazene todos os votos individuais recebidos, etiquetados com a hora de chegada aproximada ao minuto,
        // em lotes de 10 minutos. Cada lote deve ser guardado num ficheiro cujo nome identifica o período de tempo.
        int windowA = 10;
        input.transform(
                (rdd, time) ->
                    rdd.map(s -> s + "\t" + new Timestamp(time.milliseconds()).toLocalDateTime().truncatedTo(ChronoUnit.MINUTES).toString()))
            .window(Durations.minutes(windowA), Durations.minutes(windowA))
            .foreachRDD(rdd -> {
                String folder = LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES).toString().replace(":", "-");
                //rdd.coalesce(1).saveAsTextFile("hdfs://namenode:9000/logs/" + folder);
                rdd.saveAsTextFile("hdfs://namenode:9000/logs/" + folder);
            });

        // Alínea B
        // Top3: Apresente a cada minuto o top 3 dos títulos que obtiveram melhor classificação média nos últimos
        // 10 minutos.
        int windowB = 10;
        int slideB = 60;

        input.window(Durations.minutes(windowB),Durations.seconds(slideB))
            .map(t -> t.split("\t+"))
            // ID do filme, (voto, contador == 1)
            .mapToPair(v -> new Tuple2<>(v[0], new Tuple2<>(Double.parseDouble(v[1]), 1)))
            .reduceByKeyAndWindow((Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>) (i1, i2) -> new Tuple2<>(i1._1 + i2._1, i1._2 + i2._2), Durations.minutes(10), Durations.seconds(60))
            .mapToPair(v -> new Tuple2<>(v._1, v._2._1 / v._2._2))
            .foreachRDD(rdd -> {
                List<Tuple2<String, Double>> res = rdd
                    .join(movies)
                    .mapToPair(t -> new Tuple2<>(t._2._2,t._2._1))
                    .takeOrdered(3, new ComparadorTuplos());

                System.out.println(ANSI_GREEN + "TOP 3: " + res.toString() + ANSI_RESET);
            });

        // Alínea C
        // Trending: Apresente a cada 15 minutos os títulos em que o número de votos recolhido nesse período sejam
        // superiores aos votos obtidos no período anterior, independentemente do valor dos votos.
        int windowC = 15;
        input.window(Durations.minutes(windowC),Durations.minutes(windowC))
            .map(t -> t.split("\t+"))
            .mapToPair(v -> new Tuple2<>(v[0],  1))
            .reduceByKey(Integer::sum)
            .updateStateByKey((List<Integer> values, Optional<Integer> current) -> {
                if (values.isEmpty())
                    //return Optional.empty();
                    return Optional.of(0);
                else {
                    int old = current.or(0);
                    int novo = 0;
                    for(Integer i : values)
                        novo += i;
                    // Se o valor destes 15 minutos for maior do que nos 15 min passados, o valor é positivo
                    // caso contrário, é guardado o valor negativo para filtrar em seguida
                    return novo > Math.abs(old) ? Optional.of(novo) : Optional.of(-novo);
                }
            })
            .filter(p -> p._2 > 0)
            .foreachRDD(rdd -> {
                List<Tuple2<String, Integer>> res = rdd
                    .join(movies)
                    .mapToPair(t -> new Tuple2<>(t._2._2,t._2._1))
                    .collect();
                System.out.println(ANSI_GREEN + "Trending: " + res.toString() + ANSI_RESET);
            });

        sc.start();
        sc.awaitTermination();
    }
}
