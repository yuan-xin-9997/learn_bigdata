package com.atguigu.chapter05_transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/04 21:03
 * @contact: yuanxin9997@qq.com
 * @description: Flink map 算子
 */
public class Flink01_map {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // Flink程序主逻辑
        DataStreamSource<String> socketedTextStream = env.socketTextStream("hadoop102", 9999);
        socketedTextStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + " map";
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
