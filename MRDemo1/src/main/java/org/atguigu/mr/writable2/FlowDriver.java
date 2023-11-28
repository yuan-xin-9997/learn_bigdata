package org.atguigu.mr.writable2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
         // 1. 创建Job实例


        // 2. 给Job赋值
        // 2.1 关联本程序的Jar--如果是本地可以不写，在集群上运行必须写
        // 2.2 设置Mapper和Reducer类
        // 2.3 设置Mapper输出的key,value的类型
        // 2.4 设置最终输出的key,value的类型（在这里是reducer输出的key,value的类型）
        // 2.5 设置输入和输出的路径
        // 3. 运行Job

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\input1\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\output1"));

        System.out.println("======================");
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("success");
        }
    }
}
