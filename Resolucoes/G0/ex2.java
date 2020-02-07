package ggcd;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import ggcd.Pair;
import java.util.*;

public class demo {

    public static void main(String[] args) throws Exception{
        long time = System.currentTimeMillis();

        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream((new FileInputStream(args[0])))));
        
        // Titles by person -> data-7.tsv.gz
        Map<String, List<String>> mapa = new HashMap<String, List<String>>();

        String header = br.readLine();
        for(String l = br.readLine(); l != null; l = br.readLine()){
            String[] fields = l.split("\t");
            String[] out = new String[]{fields[0], fields[2]};

            if(mapa.containsKey(out[1])){
                List<String> b = mapa.get(out[1]);
                b.add(out[0]);
                mapa.put(out[1],b);
            }else {
                List<String> b2 = new ArrayList<>();
                b2.add(out[0]);
                mapa.put(out[1],b2);
            }
        }
        System.out.println("Acabei");

        System.out.println(mapa.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey, Comparator.reverseOrder()))
                .collect(Collectors.toList()));
        
        System.out.println("T= " + (System.currentTimeMillis() - time));
    }
}
