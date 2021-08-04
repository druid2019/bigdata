package com.bigdata.memo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, LongWritable,Text, FlowBean> {

    protected void reduce(Text key, Iterable<FlowBean> value, Context context) throws IOException, InterruptedException {
        System.out.println("WordCountReduce start........");
        long sumUpFlow = 0L;
        long sumDownFlow = 0L;

        //1:遍历集合，将集合中的数字相加
        for (FlowBean flowBean : value) {
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        }
        //2:将K和V写入上下文中
        context.write(key, new FlowBean(sumUpFlow, sumDownFlow));
        System.out.println("WordCountReduce end........");
    }
}
