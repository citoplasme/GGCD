package ggcd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.GregorianCalendar;
import java.util.Random;

public class ex3 {

    public static int randBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }

    public static String generate_Date(int start, int end){
        GregorianCalendar gc = new GregorianCalendar();

        int year = randBetween(start, end);

        gc.set(gc.YEAR, year);

        int dayOfYear = randBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));

        gc.set(gc.DAY_OF_YEAR, dayOfYear);

         return gc.get(gc.YEAR) + "-" + (gc.get(gc.MONTH) + 1) + "-" + gc.get(gc.DAY_OF_MONTH);
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zoo");
        Connection conn = ConnectionFactory.createConnection(conf);

        // CREATE
        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf("atores_g3"));
        t.addFamily(new HColumnDescriptor("Details"));
        admin.createTable(t);
        admin.close();

        String[] nomes = {"Joao", "Pedro", "Carlos", "Joana", "Maria", "Rui", "Jose", "Carolina", "Ricardo", "Rafael"};
        String[] apelidos = {"Pimentel", "da Silva", "Amorim", "Carvalho", "Martins", "Fernandes", "Saraiva", "Bartolomeu"};
        String[] filmes = {"Forest Gump", "Harry Potter", "Titanic", "Best Worst Movie", "Troll 2", "Tron", "Panda e os Caricas", "A Ilha do Tesouro", "A Ilha das Cabecas Cortadas"};


        // PUT
        Table ht = conn.getTable(TableName.valueOf("atores_g3"));
        for(int i = 0; i < 1000; i++) {
            // name,
            Random generator = new Random();
            int randomIndex = generator.nextInt(nomes.length);
            int randomIndex2 = generator.nextInt(apelidos.length);
            String name = nomes[randomIndex] + " " + apelidos[randomIndex2];

            // birth and
            String birth = generate_Date(1900, 2000);

            // death dates,
            String death = generate_Date(2000, 2020);

            // total number of movies,
            int total_movies = 1 + (int) (Math.random() * ((100 - 1) + 1));

            // top 3 highest ranking movies,
            StringBuilder builder = new StringBuilder();
            int m1 = generator.nextInt(filmes.length);
            builder.append(filmes[m1]);
            builder.append(",");
            int m2 = generator.nextInt(filmes.length);
            builder.append(filmes[m2]);
            builder.append(",");
            int m3 = generator.nextInt(filmes.length);
            builder.append(filmes[m3]);
            String top3 = builder.toString();

            // set of known collaborators.
            int n1 = generator.nextInt(nomes.length);
            int a1 = generator.nextInt(apelidos.length);
            String col1 = nomes[n1] + " " + apelidos[a1];

            int n2 = generator.nextInt(nomes.length);
            int a2 = generator.nextInt(apelidos.length);
            String col2 = nomes[n2] + " " + apelidos[a2];

            String collaborators = "[" + col1 + "," + col2 + "]";

            int id = i;
            String movie = "ac" + id;

            Put put = new Put(Bytes.toBytes(movie));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Name"), Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Birth"), Bytes.toBytes(birth));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Death"), Bytes.toBytes(death));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("TotalMovies"), Bytes.toBytes(Integer.toString(total_movies)));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Top3Movies"), Bytes.toBytes(top3));
            put.addColumn(Bytes.toBytes("Details"), Bytes.toBytes("Collaborators"), Bytes.toBytes(collaborators));

            ht.put(put);
        }
        ht.close();
        conn.close();
    }
}
