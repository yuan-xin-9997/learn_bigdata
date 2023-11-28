package org.atguigu.mr.outputformat;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AddressReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    /**
     * 读取的数据格式
     *          key         value
     *      www.atguigu      null
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 直接写数据
        for (NullWritable value : values) {
            context.write(key, value);
        }

    }
}
