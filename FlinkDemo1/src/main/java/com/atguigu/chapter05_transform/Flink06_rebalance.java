package com.atguigu.chapter05_transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/06 20:14
 * @contact: yuanxin9997@qq.com
 * @description: Flink rebalance 算子
 */
public class Flink06_rebalance {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(2);

        // Flink程序主逻辑
        DataStreamSource<Integer> streamSource = env.fromElements(10, 20, 11, 15, 22, 30);
        streamSource.rebalance()  // 轮询返回：nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
                                  //         return nextChannelToSendTo;
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
