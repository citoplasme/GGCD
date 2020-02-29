package ggcd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;

// Ratings -> /Users/JoaoPimentel/desktop/4ANO/GGCD/Dados/data.tsv.gz
public class Ratings {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            if(value.toString().equals("tconst\taverageRating\tnumVotes"))
                return;
            context.write(new Text(words[0]), new FloatWritable(Float.parseFloat(words[1])));
        }
    }

    public static class MyReducer extends Reducer<Text, FloatWritable,Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            Float sum = (float) 0;
            for(FloatWritable value: values) {
                sum+=value.get();
            }
            context.write(key, new FloatWritable(sum));
        }
    }

    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, FloatWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            float rating = Float.parseFloat(words[words.length - 1]);
            if(rating >= 9)
                context.write(new Text(words[0]), new FloatWritable(rating));
        }
    }

    public static class MyReducer2 extends Reducer<Text, FloatWritable,Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            for(FloatWritable value: values) {
                context.write(key, new FloatWritable(value.get()));
            }
        }
    }

    public static class MyMapper3 extends Mapper<LongWritable, Text, FloatWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            float rating = Float.parseFloat(words[words.length - 1]);
            context.write(new FloatWritable(rating), new Text(words[0]));
        }
    }

    public static class MyReducer3 extends Reducer<FloatWritable, Text, FloatWritable,Text> {
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values) {
                context.write(key, new Text(value));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();
        // ------------------------------ Job 1 -------------------------------------
        Job job = Job.getInstance(new Configuration(), "rating_per_movie");

        job.setJarByClass(Ratings.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class); // Combines the values before the reducer
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, "/Users/JoaoPimentel/desktop/4ANO/GGCD/Dados/data.tsv.gz");

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("tmp"));

        job.waitForCompletion(true);

        // ------------------------------ Job 2 -------------------------------------
        Job job2 = Job.getInstance(new Configuration(), "rating_per_movie_ge_9");

        job2.setJarByClass(Ratings.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setCombinerClass(MyReducer2.class); // Combines the values before the reducer
        job2.setReducerClass(MyReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job2, "/Users/JoaoPimentel/IdeaProjects/GGCD_G1/tmp/part-r-00000");

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("out"));

        job2.waitForCompletion(true);

        // ------------------------------ Job 3 -------------------------------------
        Job job3 = Job.getInstance(new Configuration(), "sorted");

        job3.setJarByClass(Ratings.class);
        job3.setMapperClass(MyMapper3.class);
        job3.setCombinerClass(MyReducer3.class);
        job3.setReducerClass(MyReducer3.class);

        job3.setOutputKeyClass(FloatWritable.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job3, "/Users/JoaoPimentel/IdeaProjects/GGCD_G1/out/part-r-00000");

        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3, new Path("sorted"));

        job3.waitForCompletion(true);

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        System.out.println("Nanoseconds  : " + timeElapsed);
        System.out.println("Milliseconds : " + timeElapsed / 1000000);
    }
}
