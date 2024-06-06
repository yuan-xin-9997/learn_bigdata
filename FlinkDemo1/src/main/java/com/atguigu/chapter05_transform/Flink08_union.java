package com.atguigu.chapter05_transform;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/06 20:36
 * @contact: yuanxin9997@qq.com
 * @description: Flink union 算子（三桶水倒在一起，水乳交融）
 * 可以同时多个流union在一起
 * union 算子要求两个流的数据类型必须一致
 */
public class Flink08_union {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        DataStreamSource<Integer> streamSource1 = env.fromElements(10, 11, 9, 20, 12);
        DataStreamSource<Integer> streamSource2 = env.fromElements(10, 11, 9, 20, 12);
        DataStreamSource<Integer> streamSource3 = env.fromElements(10, 11, 9, 20, 12);
        DataStream<Integer> result = streamSource1.union(streamSource3);
        result.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer value) throws Exception {
                return value + "<>";
            }
        })
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
