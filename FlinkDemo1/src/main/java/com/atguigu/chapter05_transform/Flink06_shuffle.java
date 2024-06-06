package com.atguigu.chapter05_transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/06 20:03
 * @contact: yuanxin9997@qq.com
 * @description: Flink shuffle 算子：可以一定程度打乱数据，实现数据重新分区
 */
public class Flink06_shuffle {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(2);

        // Flink程序主逻辑
        // DataStreamSource<String> socketedTextStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<Integer> streamSource = env.fromElements(10, 20, 11, 15, 22, 30);
        streamSource.shuffle()  // 随机返回 return random.nextInt(numberOfChannels);
                .print("shuffle后");

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
