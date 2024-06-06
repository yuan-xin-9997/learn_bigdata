package com.atguigu.chapter05_transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/06 20:17
 * @contact: yuanxin9997@qq.com
 * @description: Flink rescala 算子
 */
public class Flink06_rescala {
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
        streamSource.rescale()  // 与rebalance类似，但是效率更高，因为rescala不会跨节点（不会跨TaskManager）
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
