package org.atguigu.mr.compare3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 把FlowBean作为key的目的是为了排序
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private FlowBean outKey = new FlowBean();// 创建key对象
    private Text outValue = new Text();//创建value对象
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        // 1. 切割数据
        String[] line = value.toString().split("\t");
        // 2.封装key,value
        //给key赋值
        outKey.setUpFlow(Long.parseLong(line[1]));
        outKey.setDownFlow(Long.parseLong(line[2]));
        outKey.setSumFlow(Long.parseLong(line[3]));
        //给value赋值
        outValue.set(line[0]);
        //3. 将key,value写出去
        context.write(outKey, outValue);
    }
}
