package com.atguigu.chapter11.functions;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/17 19:40
 * @contact: yuanxin9997@qq.com
 * @description: Flink 自定义函数UDF -  Scalar函数 - Scalar functions map scalar values to a new scalar value.
 * 此处scala是指标量，表里面的一行元素，并不是只1个值，即进去一行，出来一行，一进一出
 *
 * ref: <a href="https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/functions/udfs/">...</a>
 *
 * User-defined functions (UDFs) are extension points to call frequently used logic or custom logic that cannot be
 * expressed otherwise in queries.
 *
 * User-defined functions can be implemented in a JVM language (such as Java or Scala) or Python. An implementer can
 * use arbitrary third party libraries within a UDF. This page will focus on
 * JVM-based languages, please refer to the PyFlink documentation for details on writing general and vectorized UDFs in Python.
 *
 *
 * Overview
 * Currently, Flink distinguishes between the following kinds of functions:
 *
 * Scalar functions map scalar values to a new scalar value.
 * Table functions map scalar values to new rows.
 * Aggregate functions map scalar values of multiple rows to a new scalar value.
 * Table aggregate functions map scalar values of multiple rows to new rows.
 * Async table functions are special functions for table sources that perform a lookup.
 */
public class Flink01_function_Scalar {
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
        Table table = tEnv.fromDataStream(stream);

        // 自定义UDF函数的使用方式
        // 1. 在TableAPI中使用
        // 1.1 内联的方式使用
//        table
//                .select($("id"), Expressions.call(MyUpper.class, $("id")).as("upper_id"))
//                .execute()
//                .print();

        // 1.2 先注册，再使用
//        tEnv.createTemporaryFunction("MyUpper", MyUpper.class);
//        tEnv.createTemporaryFunction("MyUpper", new MyUpper());
//        table
//                .select($("id"), Expressions.call("MyUpper", $("id")).as("upper_id"))
//                .execute()
//                .print();

        // 2. 在Flink SQL中使用
        tEnv.createTemporaryFunction("MyUpper", MyUpper.class);  // 一定要先注册临时表，和临时函数，才能在SQL中使用
        tEnv.createTemporaryView("sensor", table);
        tEnv.sqlQuery("select " +
                "id," +
                " MyUpper(id) uppser_id " +
                " from sensor" +
                "")
                .execute()
                .print();

    }

    // 自定义UDF函数实现类
    public static class MyUpper extends ScalarFunction {
        // 约定：方法名必须是eval
        // 参数和返回值，根据实际情况来决定
        public String eval(String str) {
            if (str != null) {
                return str.toUpperCase();
            }else{
                return null;
            }
        }
    }
}
