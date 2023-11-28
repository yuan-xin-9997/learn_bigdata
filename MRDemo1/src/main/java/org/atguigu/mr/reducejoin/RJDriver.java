package org.atguigu.mr.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ReduceJoin案例：将商品信息表中数据根据商品pid合并到订单数据表中。
 *
 */
public class RJDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());

        // =====================设置自定义分组类====================
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        // 设置常规参数
        job.setJarByClass(RJDriver.class);
        job.setMapperClass(RJMapper.class);
        job.setReducerClass(RJReducer.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\input9"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\output10"));

        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("success");
        }
    }
}
