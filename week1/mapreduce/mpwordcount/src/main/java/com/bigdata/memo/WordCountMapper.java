package com.bigdata.memo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    //map方法就是将K1和V1转为K2和V2
    /*
        参数：
            key     :K1  行偏移量
            value   :V1  每一行的文本数据
            context :   表示上下文对象
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("WordCountMapper start........");
        String line = value.toString();

        String[] arr = line.split("\\s+");
        // 以手机号为key
        String phone = arr[1];

        // 上行流量
        long upFlow = Long.parseLong(arr[7]);
        System.out.println("上行流量........." + upFlow);
        // 下行流量
        long downFlow = Long.parseLong(arr[8]);
        System.out.println("下行流量........." + downFlow);
        // 组装k,v将数据写入上下文
        context.write(new Text(phone), new FlowBean(upFlow, downFlow));
        System.out.println("WordCountMapper end........");
    }

}
