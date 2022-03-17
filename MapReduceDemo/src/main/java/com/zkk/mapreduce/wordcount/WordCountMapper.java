package com.zkk.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; // hadoop 2.0 3.0 的包，只负责计算 1.0还负责调度

import java.io.IOException;

/**
 * @author Keke
 * @create 2022-03-16 15:31
 */

/**
 * KEYIN, map阶段输入的key的类型：偏移量！LongWritable
 * VALUEIN, map阶段输入value类型：Text
 * KEYOUT, map阶段输出key类型：单词类型 Text
 * VALUEOUT map阶段输出的value类型：Intwritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //1 获取一行
        //atguigu atguigu
        String line = value.toString();
        //2 切割
        //atguigu
        //atguigu
        String[] words = line.split(" ");

        //循环写出
        for (String word:words){
            //分装outk
            outk.set(word);
            //写出
            context.write(outk,outv);
        }

    }
}
