package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/05/27 19:34
 * @contact: yuanxin9997@qq.com
 * @description: Flink流处理 处理 无界流数据数据（Socket端口模拟数据）  WordCount
 */
public class Flink01_Streaming_unbounded_WordCount {
    public static void main(String[] Args) throws Exception {
        // 1. 创建流处理执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 2. 读取数据 切分 转换
        DataStreamSource<String> socketedTextStream = executionEnvironment.socketTextStream("hadoop102", 9999);
        socketedTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        })
        // 3. 分组 sum
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                .sum(1)
                .print();

        // 4. 执行
        executionEnvironment.execute();
    }
}
