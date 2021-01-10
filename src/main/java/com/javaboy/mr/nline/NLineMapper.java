package com.javaboy.mr.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    LongWritable v = new LongWritable(1);
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //banzhang ni hao
        String line = value.toString();
        String[] words = line.split(" ");
        for (int i = 0; i < words.length; i++) {
            k.set(words[i]);
            context.write(k,v);
        }
    }
}
