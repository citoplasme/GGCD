package ggcd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class LoadMovies {

    public static class MapperMovies extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split("\t");
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("TitleType"), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("PrimaryTitle"), Bytes.toBytes(fields[2]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("OriginalTitle"), Bytes.toBytes(fields[3]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("isAdult"), Bytes.toBytes(fields[4]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("StartYear"), Bytes.toBytes(fields[5]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("EndYear"), Bytes.toBytes(fields[6]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Runtime"), Bytes.toBytes(fields[7]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Genres"), Bytes.toBytes(fields[8]));
            context.write(null, put);
        }
    }

    public static class MapperMoviesScores extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split("\t+");
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Rating"), Bytes.toBytes(fields[1]));
            context.write(null, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zoo");
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf("movies"));
        t.addFamily(new HColumnDescriptor("Details"));
        admin.createTable(t);
        admin.close();

        // -------------------- Movie Info -----------------------

        Job job = Job.getInstance(conf,"load-movies");
        job.setJarByClass(LoadMovies.class);
        job.setMapperClass(MapperMovies.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, "hdfs://namenode:9000/input/data-5.tsv.gz");

        job.setOutputFormatClass(TableOutputFormat.class);

        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "movies");

        job.waitForCompletion(true);

        // ------------------- Ratings -------------------
        Job job2 = Job.getInstance(conf,"load-ratings");
        job2.setJarByClass(LoadMovies.class);
        job2.setMapperClass(MapperMoviesScores.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Put.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job2, "hdfs://namenode:9000/input/data.tsv.gz");

        job2.setOutputFormatClass(TableOutputFormat.class);

        job2.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "movies");

        job2.waitForCompletion(true);
        conn.close();
    }

}
