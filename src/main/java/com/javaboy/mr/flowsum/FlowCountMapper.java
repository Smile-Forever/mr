package com.javaboy.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    FlowBean v = new FlowBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split("\t");
        String phoneNum = words[1];
        long upFlow = Long.parseLong(words[words.length - 3]);
        long downFlow = Long.parseLong(words[words.length - 2]);
        //v.set(upFlow,downFlow);
        k.set(phoneNum);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        context.write(k, v);
    }
}
