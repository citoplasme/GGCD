package ggcd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ggcd.Pair;
import ggcd.Tuple;

public class LoadActors {
    // Actors Info
    public static class MapperActorInfo extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            // In case its the header line
            if(key.get() == 0)
                return;
            // Otherwise
            String[] fields = value.toString().split("\t+");
            // Verify if the entry is an actor or actress
            if(fields[4].contains("actor") || fields[4].contains("actress")){
                Put put = new Put(Bytes.toBytes(fields[0]));
                put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("PrimaryName"), Bytes.toBytes(fields[1]));
                // Verify if the Birth Date is Null
                if(!fields[2].equals("\\N"))
                    put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("BirthYear"), Bytes.toBytes(fields[2]));
                // Verify if the Death Date is Null
                if(!fields[3].equals("\\N"))
                    put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("DeathYear"), Bytes.toBytes(fields[3]));
                context.write(null, put);
            }
        }
    }

    // Total Movies by Actors
    public static class MapperMoviesbyActor extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // In case its the header line
            if(key.get() == 0)
                return;
            // Otherwise
            String[] fields = value.toString().split("\t+");
            // Verify if the entry is an actor or actress or plays him/her self
            if(fields[3].contains("actor") || fields[3].contains("actress") || fields[3].contains("self")){
                context.write(new Text(fields[2]), new LongWritable(1));
            }
        }
    }

    public static class ReducerMoviesByActor extends TableReducer<Text, LongWritable, Put> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // Initialize the counter
            int counter = 0;
            for(LongWritable value : values){
                counter += value.get();
            }
            // Update the value in the table
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("TotalMovies"), Bytes.toBytes(counter));
            context.write(null, put);
        }
    }

    // Top 3 Movies by Actor

    // Movie (Side, Actor)
    public static class MapperMovieActor extends Mapper<LongWritable, Text, Text, Tuple> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // In case its the header line
            if(key.get() == 0)
                return;
            // Otherwise
            String[] fields = value.toString().split("\t+");
            if(fields[3].contains("actor") || fields[3].contains("actress") || fields[3].contains("self")){
                context.write(new Text(fields[0]), new Tuple(new Text("Actor"), new Text(fields[2])));
            }
        }
    }

    // Movie (Side, Rating)
    public static class MapperMovieRating extends Mapper<LongWritable, Text, Text, Tuple> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // In case its the header line
            if (key.get() == 0)
                return;
            // Otherwise
            String[] fields = value.toString().split("\t+");
            context.write(new Text(fields[0]), new Tuple(new Text("Rating"), new Text(fields[1])));
        }
    }

    // Movie (Side, Name)
    public static class MapperMovieName extends Mapper<LongWritable, Text, Text, Tuple> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // In case its the header line
            if (key.get() == 0)
                return;
            // Otherwise
            String[] fields = value.toString().split("\t+");
            context.write(new Text(fields[0]), new Tuple(new Text("Name"), new Text(fields[2])));
        }
    }

    // Join the values based on the movies
    public static class ReducerJoin extends Reducer<Text, Tuple,Text, Tuple> {
        @Override
        protected void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
            String rating = "", name = "";
            List<String> actors = new ArrayList<>();

            for(Tuple value: values) {
                // In case its an entry in the format: Movie Rating
                if (value.getKey().toString().equals("Rating")){
                    rating = value.getValue().toString();
                }
                // In case its an entry in the format: Movie Actor
                else if (value.getKey().toString().equals("Actor")){
                    String act = value.getValue().toString();
                    actors.add(act);
                }
                // In case its an entry in the format: Movie Name
                else if (value.getKey().toString().equals("Name")){
                    name = value.getValue().toString();
                }
            }
            // There are N actors in each movie
            for(String actor : actors){
                // Actor (Name, Rating) -> Values can't bee null
                if(!actor.equals("") && !rating.equals("") && !name.equals(""))
                    context.write(new Text(actor), new Tuple(new Text(name), new Text(rating)));
            }
        }
    }

    // Identity
    public static class MapperGroupMoviesByActor extends Mapper<Text, Tuple, Text, Tuple> {
        @Override
        protected void map(Text key, Tuple value, Context context) throws IOException, InterruptedException {
            // Identity
            context.write(key, value);
        }
    }

    // Get the top 3 list and update the table
    public static class ReducerTop3 extends TableReducer<Text, Tuple, Put> {
        @Override
        protected void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
            // Initialize the list for the actor
            List<Pair<String, Float>> movies = new ArrayList<>();
            for(Tuple value: values) {
                // Each entry in the list has to be considered
                movies.add(new Pair<>(value.getKey().toString(), Float.parseFloat(value.getValue().toString())));
            }
            // Sort the list by the rating
            movies.sort((a, b) -> b.getValue().compareTo(a.getValue()));
            // get the top 3 from the list
            List<Pair<String, Float>> top3 = movies.subList(0, movies.size() >= 3 ? 3 : movies.size());
            // Convert the list to string, this way it can be easily used in an non java application
            String top_movies = top3.toString();
            // Update the value in the table
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Top3Movies"), Bytes.toBytes(top_movies));
            context.write(null, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zoo");
        Connection conn = ConnectionFactory.createConnection(conf);


        // Create the table
        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf("actors_tp1"));
        t.addFamily(new HColumnDescriptor("Details"));
        admin.createTable(t);
        admin.close();

        //double start = System.currentTimeMillis();

        // -------------------- Actor Info -----------------------
        Job job = Job.getInstance(conf,"load-actors");
        job.setJarByClass(LoadActors.class);
        job.setMapperClass(MapperActorInfo.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        //TextInputFormat.setInputPaths(job, "hdfs://namenode:9000/input/name.basics.tsv.bz2");
        TextInputFormat.setInputPaths(job, "hdfs://namenode:9000/input/name.basics.tsv");

        job.setOutputFormatClass(TableOutputFormat.class);

        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors_tp1");

        job.waitForCompletion(true);

        // -------------------- Total Movies -----------------------

        Job job2 = Job.getInstance(conf, "total_movies_by_actor");

        job2.setJarByClass(LoadActors.class);
        job2.setMapperClass(MapperMoviesbyActor.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);

        TextInputFormat.setInputPaths(job2, "hdfs://namenode:9000/input/title.principals.tsv");
        TableMapReduceUtil.initTableReducerJob("actors_tp1", ReducerMoviesByActor.class, job2);

        job2.setReducerClass(ReducerMoviesByActor.class);
        job2.waitForCompletion(true);

        // ------------------ Top 3 Movies ------------------

        Job job3 = Job.getInstance(conf,"top_3_movies_by_actor");
        job3.setJarByClass(LoadActors.class);
        job3.setReducerClass(ReducerJoin.class);
        job3.setOutputKeyClass(Text.class);

        job3.setOutputValueClass(Tuple.class);

        //job3.setMapOutputKeyClass(Text.class);
        //job3.setMapOutputValueClass(Tuple.class);

        job3.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job3, new Path("hdfs://namenode:9000/input/title.principals.tsv"), TextInputFormat.class, MapperMovieActor.class);
        MultipleInputs.addInputPath(job3, new Path("hdfs://namenode:9000/input/title.ratings.tsv"), TextInputFormat.class, MapperMovieRating.class);
        MultipleInputs.addInputPath(job3, new Path("hdfs://namenode:9000/input/title.basics.tsv"), TextInputFormat.class, MapperMovieName.class);

        //MultipleInputs.addInputPath(job3, new Path("hdfs://namenode:9000/input/title.principals.tsv.gz"), TextInputFormat.class, MapperMovieActor.class);
        //MultipleInputs.addInputPath(job3, new Path("hdfs://namenode:9000/input/title.ratings.tsv.gz"), TextInputFormat.class, MapperMovieRating.class);
        //MultipleInputs.addInputPath(job3, new Path("hdfs://namenode:9000/input/title.basics.tsv.gz"), TextInputFormat.class, MapperMovieName.class);

        //job3.setOutputFormatClass(TextOutputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);

        //TextOutputFormat.setOutputPath(job3, new Path("hdfs://namenode:9000/output/top3movies"));
        SequenceFileOutputFormat.setOutputPath(job3, new Path("hdfs://namenode:9000/output/top3movies"));
        job3.waitForCompletion(true);

        // ----------------------------

        Job job4 = Job.getInstance(conf, "top3_update_table");

        job4.setJarByClass(LoadActors.class);
        job4.setMapperClass(MapperGroupMoviesByActor.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Tuple.class);

        job4.setInputFormatClass(SequenceFileInputFormat.class);
        //TextInputFormat.setInputPaths(job4, "hdfs://namenode:9000/output/top3movies/");
        SequenceFileInputFormat.setInputPaths(job4, "hdfs://namenode:9000/output/top3movies/");
        TableMapReduceUtil.initTableReducerJob("actors_tp1", ReducerTop3.class, job4);

        job4.setReducerClass(ReducerTop3.class);
        job4.waitForCompletion(true);

        // Delete Temporary Files
        // ...

        //System.out.println("Time to process: " + (System.currentTimeMillis() - start) + "ms");
        // Close the conection
        conn.close();
    }

}
