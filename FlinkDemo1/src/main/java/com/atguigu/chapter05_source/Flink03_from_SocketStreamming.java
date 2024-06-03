package com.atguigu.chapter05_source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/03 21:08
 * @contact: yuanxin9997@qq.com
 * @description: 从Socket端口中读取流式数据
 */
public class Flink03_from_SocketStreamming {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // Flink程序主逻辑
        // 在Windows下nc命令是ncat -lp 9999，Linux下nc命令是nc -lk 9999
        // Windows下只能连一个客户端，而且客户端断开连接 服务端也会断开
        // Linux 的nc命令+k参数可以连多个客户端，而且客户端断开连接 服务端不会断开
        env.socketTextStream("localhost", 9999)
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
