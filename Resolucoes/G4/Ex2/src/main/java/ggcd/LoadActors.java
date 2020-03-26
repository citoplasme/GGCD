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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.*;

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

    /*
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
                g.addColumn(Bytes.toBytes("Details"),Bytes.toBytes("Rating"));
                try {
                    Result getResult = this.ht.get(g);
                    String rating = (Bytes.toString(getResult.value()));
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
                } catch (IOException e) {}
            }
            String top_movies = top3.toString();
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Top3Movies"), Bytes.toBytes(top_movies));
            context.write(null, put);
        }
    }
    */

    // TOP 3 com Shuffle
    // Filme User
    public static class MyMapperTOP extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t+");
            for(int i = 1; i < fields.length; i++)
                context.write(new Text(fields[i]), new Text("L " + fields[0]));
        }
    }

    // Filme Rating
    public static class MyMapperTOP2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0)
                return;
            String[] fields = value.toString().split("\t+");
            context.write(new Text(fields[0]), new Text("R " + fields[1]));
        }
    }

    public static class MyReducerTOP extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String user = "", rating = "";

            for(Text value: values) {
                if (value.charAt(0) == 'R'){
                    rating = value.toString().replace("R ","");
                }
                else if (value.charAt(0) == 'L'){
                    user = value.toString().replace("L ","");
                }
            }
            context.write(new Text(user), new Text(key + "#" + rating));
        }
    }

    public static class MapperGroup extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t+");
            context.write(new Text(words[0]), new Text(words[1]));
        }
    }

    public static class ReducerGroup extends Reducer<Text, Text,Text, Text> {
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

    public static class MapperTop3Movies extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            if (key.get() == 0){
                return;
            }
            String[] fields = value.toString().split("\t+");

            // GET -> Ratings
            List<Pair<String, Float>> top3 = new ArrayList<>();

            for(int i = 1; i < fields.length; i++){
                if(fields[i] != null) {
                    fields[i] = fields[i].replaceAll("\\s+","");
                    String[] par = fields[i].split("#", 2);
                    if (par.length == 2 && par[1] != null && !par[1].trim().isEmpty() && par[0] != null && !par[0].trim().isEmpty()) {
                        float rating = Float.parseFloat(par[1]);
                        Pair<String, Float> p = new Pair<>(par[0], rating);
                        if (top3.size() >= 3) {
                            if (p.getValue() > top3.get(2).getValue()) {
                                top3.add(p);
                                top3.sort((a, b) -> b.getValue().compareTo(a.getValue()));
                                top3.remove(3);
                            }
                        } else top3.add(p);
                    }
                }
            }
            String top_movies = top3.toString();
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Top3Movies"), Bytes.toBytes(top_movies));
            context.write(null, put);
        }
    }

    // Assumindo que um colaborador é alguém que trabalhou com o ator
    public static class MapperActorsByMovie2 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t+");

            if (key.get() == 0)
                return;

            else {
                context.write(new Text(words[0]), new Text(words[2]));
            }
        }
    }

    public static class ReducerActorsByMovie2 extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder s = new StringBuilder();
            for(Text value: values) {
                s.append("\t");
                s.append(value.toString());
            }
            context.write(key, new Text(s.toString()));
        }
    }

    // Shuffle Join
    public static class MapperActorsMovie extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t+");
            if (key.get() == 0)
                return;
            else {
                StringBuilder s = new StringBuilder();
                for(int i = 1; i < words.length; i++){
                    s.append(words[i]);
                    s.append("\t");
                }
                context.write(new Text(words[0]), new Text("L " + s.toString()));
            }
        }
    }

    public static class MapperActorMovie extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t+");

            if (key.get() == 0)
                return;

            else {
                context.write(new Text(words[0]), new Text("R " + words[2]));
            }
        }
    }

    public static class ReducerActorsByMovie extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ator = "", lista = "";

            for(Text value: values) {
                if (value.charAt(0) == 'R'){
                    ator = value.toString().replace("R ","");
                }
                else if (value.charAt(0) == 'L'){
                    lista = value.toString().replace("L ","");
                }
            }
            context.write(new Text(ator), new Text(lista));
        }
    }

    // Agrupar por KEY
    public static class MapperGroup2 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t+");
            StringBuilder s = new StringBuilder();
            for(int i = 1; i < words.length; i++){
                s.append(words[i]);
                s.append("\t");
            }
            context.write(new Text(words[0]), new Text(s.toString()));
        }
    }

    public static class ReducerGroup2 extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder s = new StringBuilder();
            for(Text value: values) {
                s.append(value);
                //s.append("\t");
            }
            context.write(key, new Text(s.toString()));
        }
    }

    public static class MapperCollaborators extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split("\t+");

            Set<String> set = new HashSet<>();

            for(int i = 1; i < fields.length; i++){
                if(!fields[i].equals(fields[0])) { // Ator não é colaborador de si mesmo
                    set.add(fields[i]);
                }
            }
            String collaborators = set.toString();
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Collaborators"), Bytes.toBytes(collaborators));
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
        /*
        Job job4 = Job.getInstance(conf,"top_3_movies_by_actor");
        job4.setJarByClass(LoadActors.class);
        job4.setReducerClass(MyReducerTOP.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job4, new Path("hdfs://namenode:9000/output/tmp/part-r-00000"), TextInputFormat.class, MyMapperTOP.class);
        MultipleInputs.addInputPath(job4, new Path("hdfs://namenode:9000/input/data.tsv.gz"), TextInputFormat.class, MyMapperTOP2.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4, new Path("hdfs://namenode:9000/output/top3movies"));
        job4.waitForCompletion(true);
        // ----------------------------
        Job job5 = Job.getInstance(conf, "top_3_movies_by_actor_2");

        job5.setJarByClass(LoadActors.class);
        job5.setMapperClass(MapperGroup.class);
        job5.setCombinerClass(ReducerGroup.class); // Combines the values before the reducer
        job5.setReducerClass(ReducerGroup.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job5, "hdfs://namenode:9000/output/top3movies/part-r-00000");

        job5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job5, new Path("hdfs://namenode:9000/output/top3grouped/"));

        job5.waitForCompletion(true);

        */
        // Atualização da Tabela ------------------
        /*
        Job job6 = Job.getInstance(conf,"top_3_movies_by_actor_final");
        job6.setJarByClass(LoadActors.class);
        job6.setMapperClass(MapperTop3Movies.class);
        job6.setNumReduceTasks(0);
        job6.setOutputKeyClass(NullWritable.class);
        job6.setOutputValueClass(Put.class);

        job6.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job6, "hdfs://namenode:9000/output/top3grouped/part-r-00000");

        job6.setOutputFormatClass(TableOutputFormat.class);

        job6.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors_g4");

        job6.waitForCompletion(true);
        */



        // ------------------ Colaboradores ------------------
        // MOVIE LIST(ACTOR)
        /*
        Job job7 = Job.getInstance(conf, "collaborators_1");

        job7.setJarByClass(LoadActors.class);
        job7.setMapperClass(MapperActorsByMovie2.class);
        job7.setCombinerClass(ReducerActorsByMovie2.class); // Combines the values before the reducer
        job7.setReducerClass(ReducerActorsByMovie2.class);

        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);

        job7.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job7, "hdfs://namenode:9000/input/data-7.tsv.gz");

        job7.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job7, new Path("hdfs://namenode:9000/output/actors_by_movie/"));

        job7.waitForCompletion(true);
        */
        /*
        Job job8 = Job.getInstance(conf,"collaborators_2");
        job8.setJarByClass(LoadActors.class);
        job8.setReducerClass(ReducerActorsByMovie.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(Text.class);
        job8.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job8, new Path("hdfs://namenode:9000/output/actors_by_movie/part-r-00000"), TextInputFormat.class, MapperActorsMovie.class);
        MultipleInputs.addInputPath(job8, new Path("hdfs://namenode:9000/input/data-7.tsv.gz"), TextInputFormat.class, MapperActorMovie.class);
        job8.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job8, new Path("hdfs://namenode:9000/output/collaborators"));
        job8.waitForCompletion(true);
        */
        // Agrupar pela Key
        /*
        Job job9 = Job.getInstance(conf, "collaborators_group");

        job9.setJarByClass(LoadActors.class);
        job9.setMapperClass(MapperGroup2.class);
        job9.setCombinerClass(ReducerGroup2.class); // Combines the values before the reducer
        job9.setReducerClass(ReducerGroup2.class);

        job9.setOutputKeyClass(Text.class);
        job9.setOutputValueClass(Text.class);

        job9.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job9, "hdfs://namenode:9000/output/collaborators/part-r-00000");

        job9.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job9, new Path("hdfs://namenode:9000/output/collaborators_final/"));

        job9.waitForCompletion(true);
        */

        // Atualização da Tabela
        Job job10 = Job.getInstance(conf,"collaborators_put");
        job10.setJarByClass(LoadActors.class);
        job10.setMapperClass(MapperCollaborators.class);
        job10.setNumReduceTasks(0);
        job10.setOutputKeyClass(NullWritable.class);
        job10.setOutputValueClass(Put.class);

        job10.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job10, "hdfs://namenode:9000/output/collaborators_final/part-r-00000");

        job10.setOutputFormatClass(TableOutputFormat.class);

        job10.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors_g4");

        job10.waitForCompletion(true);

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
