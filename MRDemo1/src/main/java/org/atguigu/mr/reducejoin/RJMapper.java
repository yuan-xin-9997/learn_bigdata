package org.atguigu.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class RJMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean outKey = new OrderBean(); // 创建key对象
    private String fileName;// 读取的文件名

    /**
     * 在MapTask开始时候执行一次，即会在map方法前执行
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
        // InputSplit inputSplit = context.getInputSplit();//获取切片信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();//获取切片信息
        fileName = inputSplit.getPath().getName();// 获取切片所属文件的文件名字符串
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
        // 1. 切割数据
        String[] split = value.toString().split("\t");
        // 2. 封装key，value
        if ("order.txt".equals(fileName)){
            // 3列
            outKey.setId(Integer.parseInt(split[0]));
            outKey.setPid(Integer.parseInt(split[1]));
            outKey.setAmount(Integer.parseInt(split[2]));
            outKey.setPname("");//必须要设置，Shuffle会进行排序，如果不设置为Null，会引发空指针异常
        } else if ("pd.txt".equals(fileName)) {
            // 2列
//            outKey.setId(Integer.parseInt(split[0]));//不用设置，基本数据类型默认值为0
            outKey.setPid(Integer.parseInt(split[0]));
//            outKey.setAmount(Integer.parseInt(split[2]));//不用设置，基本数据类型默认值为0
            outKey.setPname(split[1]);
        }
        // 3.将key value写入环形缓冲区
        context.write(outKey, NullWritable.get());

    }
}
