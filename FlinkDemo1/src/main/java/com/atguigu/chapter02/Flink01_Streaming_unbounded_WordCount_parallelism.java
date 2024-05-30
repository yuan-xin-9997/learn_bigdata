package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/05/27 19:34
 * @contact: yuanxin9997@qq.com
 * @description: Flink流处理 处理 无界流数据数据（Socket端口模拟数据）  WordCount
 */
public class Flink01_Streaming_unbounded_WordCount_parallelism {
    public static void main(String[] Args) throws Exception {
        // Web UI
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);

        // 1. 创建流处理执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // executionEnvironment.setParallelism(1);  // 全局设置并行度

        // 2. 读取数据 切分 转换
        // socketTextStream是非并行算子
        DataStreamSource<String> socketedTextStream = executionEnvironment.socketTextStream("hadoop102", 9999); //
        socketedTextStream.flatMap(new FlatMapFunction<String, String>() {   // 匿名内部类
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).setParallelism(3)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        }).setParallelism(5)
        // 3. 分组 sum
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })  // KeyBy 不可以设置并行度
                .sum(1)
                .setParallelism(7)
                .print();// sink算子并行度默认=CPU核数，如果flin-conf.xml没有设置parallelism.default:1

        System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

        // 获取Flink执行计划
        System.out.println(executionEnvironment.getExecutionPlan());

        // 4. 执行
        executionEnvironment.execute("Flink01_Streaming_unbounded_WordCount_parallelism");
    }
}
