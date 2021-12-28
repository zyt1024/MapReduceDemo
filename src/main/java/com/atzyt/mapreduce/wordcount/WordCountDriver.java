package com.atzyt.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 设置jar路径
        job.setJarByClass(WordCountDriver.class);

        // 关联map和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置map的输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputword"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoopOut\\output1"));

        // 提交Job
        boolean result = job.waitForCompletion(true);
//        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 :1);
    }
}
