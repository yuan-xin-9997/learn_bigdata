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
* 在本地向集群提交Job
*       1. 添加集群配置参数
*       2. 打包
*       3. 注释setJarByClass，手动设置打包后的setJar
*       4. IDEA 设置 VM Options: -DHADOOP_USER_NAME=atguigu
*                    program arguments： hdfs://hadoop102:9820/input hdfs://hadoop102:9820/output
* */
public class WCDriver3 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 创建Job实例
        Configuration conf = new Configuration();
        //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        //指定MR运行在Yarn上
        conf.set("mapreduce.framework.name","yarn");
        //指定MR可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform", "true");
        //指定yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname", "hadoop103");

        Job job = Job.getInstance(conf);

        // 2. 给Job赋值
        // 2.1 关联本程序的Jar--如果是本地可以不写，在集群上运行必须写
        // job.setJarByClass(WCDriver3.class);
        job.setJar("D:\\dev\\learn_bigdata\\MRDemo1\\target\\MRDemo1-1.0-SNAPSHOT.jar");

        // 2.2 设置Mapper和Reducer类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        // 2.3 设置Mapper输出的key,value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 2.4 设置最终输出的key,value的类型（在这里是reducer输出的key,value的类型）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 2.5 设置输入和输出的路径（通过Main方法传参方式传入）
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 3. 运行Job
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("success");
        }
    }

}
