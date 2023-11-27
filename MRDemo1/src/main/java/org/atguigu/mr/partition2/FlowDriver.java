package org.atguigu.mr.partition2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/* 自定义分区案例

* 将统计结果按照手机归属地不同省份输出到不同文件中（分区）
（1）输入数据

（2）期望输出数据
	手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中。
* */

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


        /*
        * ReduceTask数量和分区的数量的关系？？假设自定义分区类中定义了5个分区
        *   ReduceTask数量=分区的数量（默认应该这么设置）
        *   ReduceTask数量>分区的数量（浪费资源，最后一个ReduceTask是空闲的，分不到任务）
        *   ReduceTask数量<分区的数量（报错，非法分区号）
        * */
        // 设置ReduceTask的数量==分区的数量，对应需求
        job.setNumReduceTasks(5);
        // 设置自定义分区类
        job.setPartitionerClass(MyPartitioner.class);


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
