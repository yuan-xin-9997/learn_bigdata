package com.atguigu.chapter11;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月15日18:40:03
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL 基础使用
 */
public class Flink04_SQL_BaseUse_1 {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("ws_001", 1000L, 10),
                new WaterSensor("ws_002", 2000L, 20),
                new WaterSensor("ws_001", 3000L, 30),
                new WaterSensor("ws_002", 4000L, 40),
                new WaterSensor("ws_001", 5000L, 50),
                new WaterSensor("ws_002", 6000L, 60)
        );
        // 2. 创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Table API
        Table table = tEnv.fromDataStream(stream);

        // 执行SQL 语句
        // 1.查询未注册的表
        // tEnv.executeSql()  // 适合DDL、增删改语句
//        tEnv.sqlQuery()  // 适合查询语句
        //tEnv.sqlQuery("select * from " + table + " where id='ws_001'").execute().print();
        // 2.查询已经注册的表
        tEnv.createTemporaryView("sensor", table);
        tEnv.sqlQuery("select * from sensor where id='ws_001'").execute().print();
    }
}
