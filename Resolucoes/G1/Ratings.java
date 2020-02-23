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
            Float sum = (float) 0;
            for(FloatWritable value: values) {
                sum+=value.get();
            }
            context.write(key, new FloatWritable(sum));
        }
    }

    /*public static class RatingComparator extends WritableComparator {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            Float v1 = ByteBuffer.wrap(b1, s1, l1).getFloat();
            Float v2 = ByteBuffer.wrap(b2, s2, l2).getFloat();

            return v1.compareTo(v2) * (-1);
        }
    }*/

    public static void main(String[] args) throws Exception {
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

        //job.setSortComparatorClass(RatingComparator.class);

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

        //job2.setSortComparatorClass(RatingComparator.class);

        job2.waitForCompletion(true);
    }
}
