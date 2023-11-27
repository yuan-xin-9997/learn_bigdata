package org.atguigu.mr.combinetextinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
* Reducer阶段会运行MapTask，MapTask会调用Reducer类型
* 作用：在该类中实现业务逻辑代码，Reduce阶段的
*
* Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
*   第一组泛型：
*       KEYIN：读取的key的类型（Mapper写出的key的类型）
*       VALUEIN：读取的value的类型（Mapper写出的value的类型）
*   第二组泛型：
*       KEYOUT：写出的key的类型（在这里是单词的类型）
*       VALUEOUT：写出的value的类型（在这里是单词的数量）
* */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable outValue = new LongWritable(); // 创建的value对象

    /**
     * 1.在reduce方法中实现需要在ReduceTask中实现的业务逻辑代码
     * 2. reduce方法在被循环调用，每调一次传入一组数据，（在这key值相同为一组）
     *
     * @param key 读取的key
     * @param values 读取的所有的value
     * @param context 上下文（在这用来将key，value写数据）
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
       // super.reduce(key, values, context);

        long sum = 0; //value的和
        // 1. 遍历所有的value
        // Iterator<LongWritable> iterator = values.iterator(); // 通过迭代器遍历
        for (LongWritable value : values) {
            // 2. 对value进行累加
            long v = value.get(); // 将LongWritable转换成long
            sum += v; // 累加value
        }
        // 3. 封装key, value
        outValue.set(sum); // 给value赋值
        // 4. 将key,value写出去
        context.write(key, outValue);
    }
}
