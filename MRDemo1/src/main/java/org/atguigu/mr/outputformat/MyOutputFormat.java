package org.atguigu.mr.outputformat;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义OutputFormat类型：
 *     1. 继承FileOutputFormat（抽象类）
 *        该抽象类中重写了checkOutputSpecs方法。在该方法中判断了①输出的路径是否设置  ②输出的路径是否存在
 *        而只一个public abstract RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)方法需要重写
 *
 *      getRecordWriter是用来获取RecordWriter对象。RecordWriter是真正用来写数据的
 *
 * public abstract class FileOutputFormat<K, V>
 *     K: reducer 写出的key的类型
 *     V: reducer 写出的value的类型
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    /**
     * 在方法中创建RecordWriter对象，并返回
     * @param job the information about the current task.
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {


        return new MyRecordWriter(job);
    }
}
