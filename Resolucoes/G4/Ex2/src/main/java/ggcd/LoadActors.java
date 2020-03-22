package ggcd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import ggcd.Pair;

public class LoadActors {

    public static class MapperActorInfo extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split("\t+");
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("PrimaryName"), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("BirthYear"), Bytes.toBytes(fields[2]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("DeathYear"), Bytes.toBytes(fields[3]));
            context.write(null, put);
        }
    }

    public static class MapperMoviesbyActor extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");

            if (value.toString().contains("tconst\tordering\tnconst\tcategory\tjob\tcharacters"))
                return;

            else {
                context.write(new Text(words[2]), new Text(words[0]));
            }
        }
    }

    public static class ReducerMoviesByActor extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder s = new StringBuilder();
            for(Text value: values) {
                s.append("\t");
                s.append(value);
            }
            context.write(key, new Text(s.toString()));
        }
    }

    public static class MapperTotalMovies extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split("\t+");
            long total = fields.length - 1;
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("TotalMovies"), Bytes.toBytes(total));
            context.write(null, put);
        }
    }

    // Assumindo que um colaborador famoso é alguém com mais de 10 filmes realizados -> FAZER
    public static class MapperCollaborators extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split("\t+");
            long total = fields.length - 1;
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("TotalMovies"), Bytes.toBytes(total));
            context.write(null, put);
        }
    }


    public static class MapperTop3Movies extends Mapper<LongWritable, Text, NullWritable, Put> {
        private Connection conn;
        private Table ht;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.conn = ConnectionFactory.createConnection(conf);
            this.ht = this.conn.getTable(TableName.valueOf("movies"));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.ht.close();
            this.conn.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split("\t+");

            // GET -> Ratings
            List<Pair<String, Float>> top3 = new ArrayList<>();
            for(int i = 1; i < fields.length; i++){
                Get g = new Get(Bytes.toBytes(fields[i]));
                Result result = ht.get(g);
                byte [] rating_bytes = result.getValue(Bytes.toBytes("Details"),Bytes.toBytes("Rating"));
                String rating = Bytes.toString(rating_bytes);
                if(rating != null){
                    float frat = Float.parseFloat(rating);
                    Pair<String, Float> p = new Pair<>(fields[i], frat);
                    if(top3.size() >= 3){
                        if(p.getValue() > top3.get(2).getValue()){
                            top3.add(p);
                            top3.sort((a, b) -> b.getValue().compareTo(a.getValue()));
                            top3.remove(3);
                        }
                    }
                    else top3.add(p);
                }
            }

            String top_movies = top3.toString();
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Top3Movies"), Bytes.toBytes(top_movies));
            context.write(null, put);
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zoo");
        Connection conn = ConnectionFactory.createConnection(conf);
        /*
        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf("actors_g4"));
        t.addFamily(new HColumnDescriptor("Details"));
        admin.createTable(t);
        admin.close();
        */

        /*
        Job job = Job.getInstance(conf,"load-actors");
        job.setJarByClass(LoadActors.class);
        job.setMapperClass(MapperActorInfo.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, "hdfs://namenode:9000/input/data-4.tsv.gz");

        job.setOutputFormatClass(TableOutputFormat.class);

        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors_g4");

        job.waitForCompletion(true);
        */

        // ------------------ Cálculo do Número de Filmes por Ator na BD -----------------------
        /*
        Job job2 = Job.getInstance(conf, "group_movies_by_actor");

        job2.setJarByClass(LoadActors.class);
        job2.setMapperClass(MapperMoviesbyActor.class);
        job2.setCombinerClass(ReducerMoviesByActor.class); // Combines the values before the reducer
        job2.setReducerClass(ReducerMoviesByActor.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job2, "hdfs://namenode:9000/input/data-7.tsv.gz");

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("hdfs://namenode:9000/output/tmp"));

        job2.waitForCompletion(true);
        */
        // ------------------ Carregamento do Número de Filmes por Ator na BD ------------------
        /*
        Job job3 = Job.getInstance(conf,"count_movies_by_actor");
        job3.setJarByClass(LoadActors.class);
        job3.setMapperClass(MapperTotalMovies.class);
        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Put.class);

        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job3, "hdfs://namenode:9000/output/tmp/part-r-00000");

        job3.setOutputFormatClass(TableOutputFormat.class);

        job3.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors_g4");

        job3.waitForCompletion(true);
        */

        // ------------------ Carregamento do Top 3 de Filmes por Ator na BD ------------------

        Job job4 = Job.getInstance(conf,"top_3_movies_by_actor");
        job4.setJarByClass(LoadActors.class);
        job4.setMapperClass(MapperTop3Movies.class);
        job4.setNumReduceTasks(0);
        job4.setOutputKeyClass(NullWritable.class);
        job4.setOutputValueClass(Put.class);

        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job4, "hdfs://namenode:9000/output/tmp/part-r-00000");

        job4.setOutputFormatClass(TableOutputFormat.class);

        job4.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors_g4");

        job4.waitForCompletion(true);



        // ------------------ Colaboradores ------------------
        /*
        Job job5 = Job.getInstance(conf,"count_movies_by_actor");
        job5.setJarByClass(LoadActors.class);
        job5.setMapperClass(MapperTotalMovies.class);
        job5.setNumReduceTasks(0);
        job5.setOutputKeyClass(NullWritable.class);
        job5.setOutputValueClass(Put.class);

        job5.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job5, "hdfs://namenode:9000/output/tmp_collaborators/part-r-00000");

        job5.setOutputFormatClass(TableOutputFormat.class);

        job5.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors_g4");

        job5.waitForCompletion(true);
        */



        conn.close();

        // Apagar ficheiros temporários
        /*
        File index = new File("/Users/JoaoPimentel/IdeaProjects/GGCD_G1/tmp");
        String[]entries = index.list();
        for(String s: entries){
            File currentFile = new File(index.getPath(),s);
            currentFile.delete();
        }
        index.delete();
        */
    }

}
