package com.atguigu.chapter05_source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/04 20:03
 * @contact: yuanxin9997@qq.com
 * @description: 用户自定义数据源
 */
public class Flink05_UDFSource {
    public static void main(String[] Args) {
        // Flink Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // Flink程序主逻辑
        env.addSource(new MySource("localhost", 9999))
                .print();

        /*
sensor_1,1607527992000,20
sensor_1,1607527993000,40
sensor_1,1607527994000,50
 */

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
