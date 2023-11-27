package org.atguigu.mr.partition3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/*
* 自定义分区
*  实现WordCount按照a-p，q-z分区
* Partitioner<KEY, VALUE>
*    Key : map 输出的key
*    value： map输出的value
* */
public class MyPartitioner extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        String word = text.toString();
        // 实现方式一：
//
//        char c = word.charAt(0);
//        if ((c >= 'a' && c <= 'p') || (c >= 'A' && c <= 'P')){
//            return 0;
//        }else {
//            return 1;
//        }
        // 实现方式二（正则表达式）
        if (word.matches("^[a-pA-p].*")){
            return 0;
        }else {
            return 1;
        }
    }
}
