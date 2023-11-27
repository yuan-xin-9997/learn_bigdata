package org.atguigu.mr.compare3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 在写数据的时候将key,value的顺序进行交换
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 1. 遍历values
        for (Text value : values) {
            // 写出数据
            context.write(value, key);
        }
    }
}
