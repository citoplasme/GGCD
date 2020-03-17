package ggcd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ex1 {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zoo");
        Connection conn = ConnectionFactory.createConnection(conf);

        // CREATE
        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf("votos_g3"));
        t.addFamily(new HColumnDescriptor("Details"));
        admin.createTable(t);
        admin.close();

        // PUT
        Table ht = conn.getTable(TableName.valueOf("votos_g3"));
        for(int i = 0; i < 1000; i++) {
            int vote = (int) (Math.random() * ((10 - 0) + 1));
            int id = 1 + (int)(Math.random() * ((1000 - 1) + 1));
            String movie = "tt" + id;
            List<String> put_list = new ArrayList<>();

            // GET
            Get g = new Get(Bytes.toBytes(movie));
            Result result = ht.get(g);
            byte [] value = result.getValue(Bytes.toBytes("Details"),Bytes.toBytes("Vote"));
            String lista = Bytes.toString(value);
            if(lista != null){
                String values = lista.substring(1, lista.length()-1);
                String str[] = values.split("\\s*,\\s*");
                for(String s : str) {
                    put_list.add(s);
                }
            }
            put_list.add(Integer.toString(vote));
            String votos = put_list.toString();

            Put put = new Put(Bytes.toBytes(movie));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Vote"), Bytes.toBytes(votos));

            ht.put(put);
        }
        ht.close();
        conn.close();
    }
}
