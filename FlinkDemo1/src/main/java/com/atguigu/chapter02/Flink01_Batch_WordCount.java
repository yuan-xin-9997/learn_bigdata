package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/05/23 21:24
 * @contact: yuanxin9997@qq.com
 * @description: flink批处理wordcount
 */
public class Flink01_Batch_WordCount {
    public static void main(String[] Args) throws Exception {
        // File directory = new File("");//设定为当前文件夹
        // System.out.println(directory.getAbsolutePath());//获取绝对路径

        // 1. 获取Flink批处理执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取文件
        DataSource<String> dataSource = executionEnvironment.readTextFile("FlinkDemo1/input/words.txt");

        // 3. 切分数据，在这里使用flatmap进行切分，flatmap 一变多
        // 经过flatmap之后，数据已经变成一个一个的单词
        FlatMapOperator<String, String> flatMapOperator = dataSource.flatMap(new FlatMapFunction<String, String>() {
            // Collector是Flink中的收集器，作用就是 我们把处理好要放入流中的数据 扔到Collector中即可
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                // 将切分后的单词 放入Collector中
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        // 在这一步，我们希望把一个一个的单词，变成Tuple2(word,1)的形式
        // 需要借助map操作
        MapOperator<String, Tuple2<String, Long>> mapOperator = flatMapOperator.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });


        // 4. 对tuple第1个元素进行分组，并对tuple第2个元素进行sum操作
        AggregateOperator<Tuple2<String, Long>> results = mapOperator.groupBy(0).sum(1);

        // 5. 打印结果
        results.print();
    }
}
