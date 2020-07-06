package ggcd;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tuple implements Writable {

    private Text first;
    private Text second;

    public Tuple() {
        first = new Text();
        second = new Text();
    }

    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }


    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }


    public Text getKey() {
        return first;
    }

    public void setKey(Text t) {
        this.first = t;
    }

    public Text getValue() {
        return second;
    }

    public void setValue(Text t) {
        this.second = t;
    }

    public Tuple(Text key, Text value) {
        this.first = key;
        this.second = value;
    }

    @Override
    public String toString() {
        return first + "\t" + second ;
    }
}