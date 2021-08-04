package com.bigdata.memo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        System.out.println("WordCountDriver start........");
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        // 创建一个job任务
        Job job = Job.getInstance(conf, "word_count_ging");
        // 配置job任务对象
        job.setJarByClass(WordCountDriver.class);
        // 2.设置Mapper类
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

//        job.setOutputFormatClass(TextOutputFormat.class);

        // 指定输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 运行
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? "success" : "fail");
        System.exit(result ? 0 : 1);
        System.out.println("WordCountDriver end........");
        long end = System.currentTimeMillis();
        long cost = end - start;
        System.out.println("run time costs:" + cost);

    }
}
