package org.atguigu.mr.partition3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
* 程序入口，
* 1.创建Job实例并运行（在本地运行）
*
** 自定义分区
*  实现WordCount按照a-p，q-z分区
* */
public class WCDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 创建Job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置ReduceTask的数量
        job.setNumReduceTasks(2);
        // 设置自定义分区类
        job.setPartitionerClass(MyPartitioner.class);

        // 2. 给Job赋值
        // 2.1 关联本程序的Jar--如果是本地可以不写，在集群上运行必须写
        job.setJarByClass(WCDriver.class);
        // 2.2 设置Mapper和Reducer类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        // 2.3 设置Mapper输出的key,value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 2.4 设置最终输出的key,value的类型（在这里是reducer输出的key,value的类型）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 2.5 设置输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\input\\"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\output"));

        // 3. 运行Job
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("success");
        }
    }

}
