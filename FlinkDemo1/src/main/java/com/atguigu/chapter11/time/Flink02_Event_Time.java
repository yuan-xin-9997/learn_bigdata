package com.atguigu.chapter11.time;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月15日18:40:03
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL -  在表中添加时间属性（本demo为添加事件时间）
 */
public class Flink02_Event_Time {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        // 先添加水印
         DataStream<WaterSensor> stream = env.fromElements(
                new WaterSensor("ws_001", 1000L, 10),
                new WaterSensor("ws_002", 2000L, 20),
                new WaterSensor("ws_001", 3000L, 30),
                new WaterSensor("ws_002", 4000L, 40),
                new WaterSensor("ws_001", 5000L, 50),
                new WaterSensor("ws_002", 6000L, 60)
        )
                 .assignTimestampsAndWatermarks(
                         WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                 .withTimestampAssigner( (value, timestamp) -> value.getTs())
                 );
        // 2. 创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 添加时间时机1 - 在流转成表的时候添加时间（添加一个新的字段）
//        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts").rowtime(), $("vc"));

        table.printSchema();
        table.execute().print();

    }
}
