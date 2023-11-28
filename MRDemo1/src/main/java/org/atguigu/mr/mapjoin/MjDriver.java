package org.atguigu.mr.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Map Join案例：Map Join适用于一张表十分小、一张表很大的场景。
 *
 * 在Map端缓存多张表，提前处理业务逻辑，这样增加Map端业务，减少Reduce端数据的压力，尽可能的减少数据倾斜。
 */
public class MjDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());

        // 添加缓存文件的路径
        job.addCacheFile(new URI("file:///D:/dev/learn_bigdata/MRDemo1/input9/pd.txt"));
        // 如果没有ReduceTask任务，可以将ReduceTask的数量设置成0，将不再对key进行排序。否则MR依然会对输出的key进行排序
        // 如果不设置ReduceTask数量，那么默认ReduceTask的数量为1
//        job.setNumReduceTasks(1);
        job.setNumReduceTasks(0);

        // 设置常规参数
        job.setMapperClass(MJMapper.class);
        job.setJarByClass(MjDriver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 没有Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

//        FileInputFormat.setInputPaths(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\input9\\order.txt"));
        FileInputFormat.setInputPaths(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\input10"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\dev\\learn_bigdata\\MRDemo1\\output9"));

        boolean flag = job.waitForCompletion(true);
        if(flag){
            System.out.println("success");
        }
    }
}
