package com.atguigu.chapter05_RuntimeMode;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/13 21:35
 * @contact: yuanxin9997@qq.com
 * @description: Flink RuntimeMode 运行模式
 */
public class Flink01_RuntimeMode {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // 代码设置运行模式
        // 执行模式有3个选择可配:
        //1STREAMING(默认)
        //2BATCH  批处理模式
        //3AUTOMATIC
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);  // 批处理模式
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);  // 批处理模式
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);  // 批处理模式

        // 如果是流处理模式（比如socketTextStream），则只可以使用Automatic、Streaming模式，使用Batch模式会报错
        // 如果是批处理模式（比如readTextFile），则只可以使用Automatic模式、Batch模式、Streaming模式

        // Flink程序主逻辑
//        env.socketTextStream("localhost", 9999)
        env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\words.txt")
//        env.socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : line.split(" ")) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t->t.f0)
                .sum(1)
                .print();
        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
