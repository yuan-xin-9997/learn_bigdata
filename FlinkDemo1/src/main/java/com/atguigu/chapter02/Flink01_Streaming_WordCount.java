package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/05/27 18:49
 * @contact: yuanxin9997@qq.com
 * @description: Flink使用有界流的方式处理WordCount
 */
public class Flink01_Streaming_WordCount {
    public static void main(String[] Args) throws Exception {
        // 1. 创建流式执行环境，并拿到数据
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = executionEnvironment.readTextFile("FlinkDemo1/input/words.txt");
        executionEnvironment.setParallelism(1);  // 并行度

        // 2. 对单词进行切分，封装成(word,1L)
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        })

        // 3. 对相同单词进行分组，并进行sum统计。IDEA ctrl+p可以进行参数列表提示
                // keyby操作不会改变数据 相当于 数据经过keyBy之后 给数据打上一个key标签，数据本身不会有任何变化
                // key相同的 会在后面被分到同一个组中
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    // 从传进来的数据中 获取key
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        // 现在把tuple2中的首位进行返回，那么每个数据带有各自的key
                        return tuple2.f0;
                    }
                })
                .sum(1)
                .print();

        // 4. 执行，与批处理不同，此处必须给它明确的执行命令，Flink才会开时处理
        executionEnvironment.execute();
    }
}
