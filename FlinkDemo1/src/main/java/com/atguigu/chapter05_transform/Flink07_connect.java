package com.atguigu.chapter05_transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/06 20:23
 * @contact: yuanxin9997@qq.com
 * @description: Flink connect 算子（同床异梦，一桶水和一桶油倒在一起）
 * 1. Connect 算子可以将两个流连接起来，形成一个 ConnectedStreams 对象。
 * 2. 两个流的数据类型可以不一样
 */
public class Flink07_connect {
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
        DataStreamSource<String> streamSource2 = env.fromElements("a", "b", "c", "d", "e");
        ConnectedStreams<Integer, String> connected = streamSource1.connect(streamSource2);
        connected.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value + "<";
            }

            @Override
            public String map2(String value) throws Exception {
                return value + ">";
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
