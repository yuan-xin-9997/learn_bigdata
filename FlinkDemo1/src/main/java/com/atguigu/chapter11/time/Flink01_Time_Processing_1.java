package com.atguigu.chapter11.time;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月15日18:40:03
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL -  在表中添加时间属性（本demo为添加处理时间-DDL）
 */
public class Flink01_Time_Processing_1 {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        // 2. 创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 添加时间时机2 - 在DDL语句中添加时间
        tEnv.executeSql("CREATE TABLE sensor (" +
                "id STRING, " +
                "ts BIGINT, " +
                "vc INT, " +
                " pt as proctime() " +
                ") " +
                "WITH ('connector' = 'filesystem', 'path' = 'FlinkDemo1/input/sensor.json', 'format' = 'json')")
                ;

        tEnv.sqlQuery("select * from sensor").execute().print();

    }
}
