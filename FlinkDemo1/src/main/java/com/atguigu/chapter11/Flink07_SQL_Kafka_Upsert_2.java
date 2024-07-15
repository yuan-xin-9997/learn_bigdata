package com.atguigu.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月15日18:40:03
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL -  Upsert Kafka 读取Kafka数据
 */
public class Flink07_SQL_Kafka_Upsert_2 {
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

        // DDL语句
//        tEnv.executeSql("CREATE TABLE sensor (id STRING, ts BIGINT, vc INT) " +
//                "WITH (" +
//                "  'connector' = 'kafka', " +
//                "  'topic' = 's3',\n" +
//                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
//                "  'properties.group.id' = 'atguigu',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'json'" +
//                ")")
//                ;

        tEnv.executeSql("CREATE TABLE sensor (" +
                "  id STRING, " +
//                "ts BIGINT, " +
                "  vc INT,  " +
                "  primary key(id) not enforced  " +
                " ) " +
                "WITH (" +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = 's3',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
//                "  'properties.group.id' = 'atguigu',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")")
                ;
        tEnv.sqlQuery("select * from sensor").execute().print();


    }
}
