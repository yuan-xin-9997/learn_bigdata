package com.atguigu.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月15日18:40:03
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL -  Upsert Kafka 将数据写入到Kafka
 */
public class Flink07_SQL_Kafka_Upsert {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20001);

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
        Table result = tEnv.sqlQuery("SELECT id,sum(vc) as sum_vc FROM sensor group by id");  // 如果有聚合，则连接器为kafka的话，其结果无法写入到Kafka
        //result.execute().print();
        // Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED  controls if the constraint checks are performed on the incoming/outgoing data. Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode
        tEnv.executeSql("CREATE TABLE abc (" +
                "   id STRING," +
//                "   ts BIGINT," +
                "   vc INT," +
                " primary key(id) not enforced" +
                " ) " +  // Flink 对主键不进行约束校验，因为Flink不拥有数据。此处加主键的目的：保证将来相同主键的数据进入到同一分区，主键为成为Kafka的key
                "WITH (" +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = 's3',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json' " +
                ")");
         result.executeInsert("abc");  // 按照顺序写入，不检验字段名
        // tEnv.executeSql("insert into abc select * from " +  result);  // 按照顺序写入，不检验字段名
    }
}
