package com.atguigu.chapter05_source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/03 20:52
 * @contact: yuanxin9997@qq.com
 * @description: 从HDFS中读取文件数据
 */
public class Flink02_fromHDFS {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 从HDFS读取文件数据
        env.readTextFile("hdfs://hadoop102:8020/input/student.txt")
                .print();
        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
