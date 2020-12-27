package com.javaboy.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map阶段
 * 参数1：输入数据的key
 * 参数2：输入数据的value
 * 参数3：输出数据的类型，xbvern，1  javaboy，1
 * 参数4：输出数据的value类型
 */
public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    //输出k
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //1.获取一行
        String line = value.toString();
        //2.切割单词
        String[] words = line.split(" ");
        //3.循环输出
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }
    }
}
