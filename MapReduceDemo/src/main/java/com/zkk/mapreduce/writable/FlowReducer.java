package com.zkk.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Keke
 * @create 2022-03-27 20:37
 */
public class FlowReducer extends Reducer<Text,FlowBean, Text,FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //1 遍历集合累加值
        long totalUp = 0;  // 这个变量必须放在这个位置
        long totaldown = 0; // 这个变量必须放在这个位置
        for (FlowBean value: values){
            totalUp += value.getUpFlow();
            totaldown += value.getDownFlow();
        }
        //2 封装outk ,outv
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totaldown);
        outV.setSumFlow();
        //
        context.write(key,outV);
    }
}
