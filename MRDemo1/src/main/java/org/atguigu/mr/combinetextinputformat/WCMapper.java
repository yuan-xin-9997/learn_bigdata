package org.atguigu.mr.combinetextinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* Mapper阶段会运行MapTask，MapTask会调用Mapper类型
* 作用：在该类中实现业务逻辑代码，Map阶段的
*
*  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
*      第一组泛型
*           KEYIN：读取数据时的偏移量的类型
*           VALUEIN：读取的一行一行的数据的类型
*      第二组泛型
*           KEYOUT：写出的key的类型（在这里是单词的类型）
*           VALUEOUT：写出的value的类型（在这里是单词的数量的类型）
* */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text outKey = new Text(); // 3.1 创建Key对象
    private LongWritable outValue = new LongWritable(); // 3.2创建Value对象

    /**
     * 1. 在map方法中实现需要在MapTask中实现的业务逻辑代码
     * 2. 该方法在被循环调用，每调用一次传入一行数据
     * @param key  读取数据时的偏移量
     * @param value 读取的一行一行的数据
     * @param context 上下文，在这里用来将key,value写数据
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // super.map(key, value, context);
        // 1. 将数据进行切割
        // 1.1. 将Text转成String--为了使用String API进行处理
        String line = value.toString();
//        String[] words = line.split(" ");
        String[] words = line.split(";");
        // 2. 遍历数据
        for (String word : words) {
            // 3. 封装key,value
            outKey.set(word);  // 给Key赋值
            outValue.set(1);  // 给Value赋值
            // 4. 将key,value写出去
            context.write(outKey, outValue);
        }
    }
}
