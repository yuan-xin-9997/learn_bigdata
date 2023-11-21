package org.atguigu.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
* 程序入口，
* 在集群上跑Job
*
*       通过Main方法传参方式传入
*       maven打包
*       将jar包上传到集群服务器
*       在集群服务器上执行命令 hadoop jar xxx.jar 要运行的全类名 输入路径 输出路径
*           举例：hadoop jar MRDemo1-1.0-SNAPSHOT.jar org.atguigu.mr.wordcount.WCDriver2 /input /output
*
* */
public class WCDriver2 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 创建Job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 给Job赋值
        // 2.1 关联本程序的Jar--如果是本地可以不写，在集群上运行必须写
        job.setJarByClass(WCDriver2.class);
        // 2.2 设置Mapper和Reducer类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        // 2.3 设置Mapper输出的key,value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 2.4 设置最终输出的key,value的类型（在这里是reducer输出的key,value的类型）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 2.5 设置输入和输出的路径（==========通过Main方法传参方式传入=============）
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 3. 运行Job
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("success");
        }
    }

}
