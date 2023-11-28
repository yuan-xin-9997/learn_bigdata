package org.atguigu.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义写文件RecordWriter的类
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguigu;  // 创建流
    private FSDataOutputStream other; // 创建流

    /**
     * 构造器，带参数
     * 创建流
     */
    public MyRecordWriter(TaskAttemptContext job) throws IOException {
        try{
            // new FileOutputStream(); 写到本地，不能写到HDFS
        // 1. 创建客户端对象（文件系统对象，用来操作HDFS）
        FileSystem fs = FileSystem.get(job.getConfiguration());
        // 2. 创建流
        // 2.1 获取输出路径
        Path outputPath = FileOutputFormat.getOutputPath(job);
        // 2.2 拼接输出目录和输出文件
        atguigu = fs.create(new Path(outputPath, "atguigu.txt"));
        other = fs.create(new Path(outputPath, "other.txt"));

        }catch (Exception e){
            // 打印异常信息
            e.printStackTrace();
            // 将编译时异常转为运行时异常
            throw new RuntimeException(e.getMessage());
        }

    }
    /**
     * 用来写数据，该方法在被循环调用，每调用一次传入一个键值对
     * @param key the key to write. reduce写的key
     * @param value the value to write.  reduce写的value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String address = key.toString() + "\n";
        // 判断是否包含atguigu
        if (address.contains("atguigu")){
            // 写到atguigu.txt
            atguigu.write(address.getBytes());
        }else {
            // 写到other.txt
            other.write(address.getBytes());
        }
    }

    /**
     * 关闭资源: 该方法会在写数据的操作结束后会被调用
     * @param context the context of the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // 3. 关闭资源
        if (other != null) {
            other.close();
        }
        if(atguigu != null){
        atguigu.close();
        }
    }
}
