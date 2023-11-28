package org.atguigu.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.runners.model.TestClass;

import java.io.IOException;

public class AddressDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置使用自定义输出类OutputFormat，如果不设置则使用的是TextOutputFormat
        job.setOutputFormatClass(MyOutputFormat.class);

        //设置常规参数
        job.setMapperClass(AddressMapper.class);
        job.setReducerClass(AddressReducer.class);
        job.setJarByClass(AddressDriver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\input4"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\output5"));

        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("success");
        }
    }
}
