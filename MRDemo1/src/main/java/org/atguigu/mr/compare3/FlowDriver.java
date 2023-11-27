package org.atguigu.mr.compare3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 基于流量倒序排序的基础上，
 * 要求每个省份手机号输出的文件中按照总流量内部排序。
 * 	基于前一个需求，增加自定义分区类，分区按照省份手机号设置。
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.创建Job实例
        Job job = Job.getInstance(new Configuration());

        // 2.设置参数
        job.setJarByClass(FlowDriver.class); // Driver的类
        job.setMapperClass(FlowMapper.class); // MapTask的类
        job.setReducerClass(FlowReducer.class);//ReduceTask的类
        job.setMapOutputKeyClass(FlowBean.class);//MapTask类输出的Key的类
        job.setMapOutputValueClass(Text.class);//MapTask类输出的Value的类
        job.setOutputKeyClass(Text.class);//ReduceTask类输出的Key的类
        job.setOutputValueClass(FlowMapper.class);//ReduceTask类输出的Value的类
        FileInputFormat.setInputPaths(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\input2"));//数据读取路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\output3"));//数据输出路径

        // =============设置自定义分区====================
        job.setPartitionerClass(MyPartitioner.class);
        // 设置ReduceTask数量
        job.setNumReduceTasks(5);

        // 3.执行job
        boolean flag = job.waitForCompletion(true);
        if(flag){
            System.out.println("success");
        }
    }
}
