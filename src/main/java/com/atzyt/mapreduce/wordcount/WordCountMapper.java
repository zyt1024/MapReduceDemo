package com.atzyt.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * atguigu atguigu
 * ss ss
 * cls cls
 * jiao
 * banzhang
 * xue
 * hadoop
 *
 * KEYIN,map阶段的键输入类型 LongWritable
 * VALUEIN, map阶段的输入value类型: Text
 * KEYOUT,map阶段的键输出类型 : Text
 * VALUEOUT map阶段的值输出类型 :IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
    private Text outK =  new Text();
    private IntWritable outV = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取1行
        String line = value.toString();

        // 切割
        String[] words = line.split(" ");

        // 循环写出
        for (String word : words){

            outK.set(word);

            // 写出
            context.write(outK,outV);
        }
    }
}
