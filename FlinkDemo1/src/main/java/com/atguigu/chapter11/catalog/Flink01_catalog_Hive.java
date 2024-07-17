package com.atguigu.chapter11.catalog;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/16 21:58
 * @contact: yuanxin9997@qq.com
 * @description: Flink Catalog hive
 *
 * 11.6Catalog
 * Catalog 提供了元数据信息，例如数据库、表、分区、视图以及数据库或其他外部系统中存储的函数和信息。
 * 数据处理最关键的方面之一是管理元数据。 元数据可以是临时的，例如临时表、或者通过 TableEnvironment 注册的 UDF。 元数据也可以是持久化的，例如 Hive Metastore 中的元数据。Catalog 提供了一个统一的API，用于管理元数据，并使其可以从 Table API 和 SQL 查询语句中来访问。
 * 前面用到Connector其实就是在使用Catalog
 * 11.6.1Catalog类型
 * GenericInMemoryCatalog
 * GenericInMemoryCatalog 是基于内存实现的 Catalog，所有元数据只在 session 的生命周期内可用。
 * JdbcCatalog
 * JdbcCatalog 使得用户可以将 Flink 通过 JDBC 协议连接到关系数据库。PostgresCatalog 是当前实现的唯一一种 JDBC Catalog。
 * HiveCatalog
 * HiveCatalog 有两个用途：作为原生 Flink 元数据的持久化存储，以及作为读写现有 Hive 元数据的接口。 Flink 的 Hive 文档 提供了有关设置 HiveCatalog 以及访问现有 Hive 元数据的详细信息。
 * 11.6.2HiveCatalog
 * 导入需要的依赖
 * <dependency>
 *     <groupId>org.apache.flink</groupId>
 *     <artifactId>flink-connector-hive_${scala.binary.version}</artifactId>
 *     <version>${flink.version}</version>
 * </dependency>
 * <!-- Hive Dependency -->
 * <dependency>
 *     <groupId>org.apache.hive</groupId>
 *     <artifactId>hive-exec</artifactId>
 *     <version>3.1.2</version>
 * </dependency>
 * 在hadoop162启动hive元数据
 * nohup hive --service metastore >/dev/null 2>&1 &
 *
 *
 *
 *
 * 注意：QA
 * * 错误信息：Permission denied: user=yuanx, access=WRITE, inode="/input":atguigu:supergroup:drwxr-xr-x
 * * 解决方案：
 * *   1. 设置权限
 * *   2. 设置操作HDFS文件系统的用户，IDEA VM options添加“-DHADOOP_USER_NAME=atguigu”
 * 或者
 * System.setProperty("HADOOP_USER_NAME", "atguigu");
 */
public class Flink01_catalog_Hive {
    public static void main(String[] Args) {
        // 设置环境变量
        System.setProperty("HADOOP_USER_NAME", "atguigu");

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
                        new WaterSensor("ws_002", 6001L, 60)
//                        ,new WaterSensor("ws_002", 9001L, 60),
//                        new WaterSensor("ws_002", 10001L, 60),
//                        new WaterSensor("ws_002", 15001L, 60),
//                        new WaterSensor("ws_002", 16001L, 60),
//                        new WaterSensor("ws_002", 20001L, 60),
//                        new WaterSensor("ws_002", 25001L, 60)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((value, timestamp) -> value.getTs())
                );
        // 创建流表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // DDL创建表
        tEnv.executeSql("CREATE TABLE person (id STRING, ts BIGINT, vc INT) " +
                "WITH ('connector' = 'filesystem', 'path' = 'FlinkDemo1/input/sensor.json', 'format' = 'json')")
                ;

        // 2. 创建Hive Catalog
        HiveCatalog hc = new HiveCatalog("hive", "gmall", "FlinkDemo1/input");
        // 3. 向表环境注册HiveCataLog
        tEnv.registerCatalog("hive", hc);
        tEnv.useCatalog("hive");
        tEnv.useDatabase("gmall");
        // 4. 使用HiveCataLog去读取hive数据
        tEnv.useCatalog("default_catalog");
        tEnv.sqlQuery("select " +
                " * " +
                //" from hive.gmall.person ")
                " from person ")
//                " from default_catalog.default_database.person ")
                .execute()
                .print();
    }
}
