package ggcd;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.zip.GZIPInputStream;

import ggcd.Pair;
import java.util.*;

public class demo {

    public static void main(String[] args) throws Exception{
        long time = System.currentTimeMillis();

        // Top 10 most Popular -> data.tsv.gz
        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream((new FileInputStream(args[0])))));

        List<Pair<String, Integer>> lista = new ArrayList<Pair<String, Integer>>();
        String header = br.readLine();
        for(String l = br.readLine(); l != null ; l = br.readLine()){
            String[] fields = l.split("\t");
            String[] out = new String[]{fields[0], fields[2]};
            //String r = String.join("\t", out);
            //System.out.println(r);

            Pair<String, Integer> p = new Pair<String, Integer>(out[0], Integer.parseInt(out[1]));
            if(lista.size() >= 10){
                if(p.getValue() > lista.get(9).getValue()){
                    lista.add(p);
                    //lista.sort((a, b) -> a.getValue() < b.getValue() ? 1 : a.getValue() == b.getValue() ? 0 : -1);
                    lista.sort((a, b) -> b.getValue().compareTo(a.getValue()));
                    lista.remove(10);
                }
            }
            else lista.add(p);
        }
        System.out.println(lista);
        System.out.println("T= " + (System.currentTimeMillis() - time));
    }
}
