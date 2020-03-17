package ggcd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class ex4 {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zoo");
        Connection conn = ConnectionFactory.createConnection(conf);

        // SCAN
        Table ht = conn.getTable(TableName.valueOf("atores_g3"));

        Scan s = new Scan();

        s.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Name"));
        s.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Birth"));
        s.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Death"));
        s.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("TotalMovies"));
        s.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Top3Movies"));
        s.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Collaborators"));


        ResultScanner scanner = ht.getScanner(s);
        try {
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                //System.out.println(rr);

                System.out.println("{");
                byte[] nome = rr.getValue(Bytes.toBytes("Details"), Bytes.toBytes("Name"));
                String name = Bytes.toString(nome);
                System.out.println("\tName: " + name);

                byte[] nasc = rr.getValue(Bytes.toBytes("Details"), Bytes.toBytes("Birth"));
                String birth = Bytes.toString(nasc);
                System.out.println("\tBirth: " + birth);

                byte[] mort = rr.getValue(Bytes.toBytes("Details"), Bytes.toBytes("Death"));
                String death = Bytes.toString(mort);
                System.out.println("\tDeath: " + death);

                byte[] tot = rr.getValue(Bytes.toBytes("Details"), Bytes.toBytes("TotalMovies"));
                String total = Bytes.toString(tot);
                System.out.println("\tTotalMovies: " + total);

                byte[] top = rr.getValue(Bytes.toBytes("Details"), Bytes.toBytes("Top3Movies"));
                String top3 = Bytes.toString(top);
                System.out.println("\tTop3Movies: " + top3);

                byte[] col = rr.getValue(Bytes.toBytes("Details"), Bytes.toBytes("Collaborators"));
                String collaborators = Bytes.toString(col);
                System.out.println("\tCollaborators: " + collaborators);
                System.out.println("}");
            }
        } finally {
            scanner.close();
        }

        ht.close();
        conn.close();
    }
}
