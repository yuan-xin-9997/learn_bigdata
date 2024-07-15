package com.atguigu.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月15日18:40:03
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL 读写Kafka
 */
public class Flink06_SQL_Kafka {
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
        tEnv.executeSql("CREATE TABLE sensor (id STRING, ts BIGINT, vc INT) " +
                "WITH (" +
                "  'connector' = 'kafka', " +
                "  'topic' = 's1',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'atguigu',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'" +
                ")")
                ;

        // 3. 执行SQL查询，并将查询得到的结果写入到表中（此表关联一个Kafka主题）
        Table result = tEnv.sqlQuery("SELECT * FROM sensor where id = 'sensor_1'");
        //result.execute().print();
        tEnv.executeSql("CREATE TABLE abc (id STRING, ts BIGINT, vc INT) " +
                "WITH (" +
                "  'connector' = 'kafka', " +
                "  'topic' = 's2',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'atguigu',\n" +
                "  'format' = 'json'" +
                ")");
         result.executeInsert("abc");  // 按照顺序写入，不检验字段名
        // tEnv.executeSql("insert into abc select * from " +  result);  // 按照顺序写入，不检验字段名
    }
}
