package com.javaboy.mr.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NLineDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //输入输出路径
        args = new String[]{"/Users/xbvern/Downloads/nlineinputformat/","/Users/xbvern/Downloads/output1"};
        //获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置每个切片InputSplit 中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job,3);
        //使用NLineInputFormat处理记录数
        job.setInputFormatClass(NLineInputFormat.class);
        //设置jar包位置 关联mapper 和 reduce
        job.setJarByClass(NLineDriver.class);
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);
        //设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job任务
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
