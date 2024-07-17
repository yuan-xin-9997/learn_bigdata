package com.atguigu.chapter11.functions;

import com.atguigu.chapter05_source.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/17 19:40
 * @contact: yuanxin9997@qq.com
 * @description: Flink 自定义函数UDF -  Table函数 - Table functions map scalar values to new rows.
 * 此处Table Function，即制表函数，一进多出，进入一行一列，出来多行多列
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
public class Flink02_function_Table {
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
                        new WaterSensor("ab a c", 1000L, 10),
                        new WaterSensor("a b c", 2000L, 20),
                        new WaterSensor("c d                       e", 3000L, 30),
                        new WaterSensor("aa", 4000L, 40),
                        new WaterSensor("a ba", 5000L, 50),
                        new WaterSensor("ws_002", 6001L, 60),
                        new WaterSensor("atguigu ab", 6001L, 60)
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

        // 1.2 先注册，再使用
//        tEnv.createFunction("mySplit", MySplit.class);
//        table  // table 和 制表生成的表join在一起 - !!!内连接!!!
//                .joinLateral(Expressions.call("mySplit", $("id")))  // 把传入的字符串的id的值，做成了表
//                                .select($("id"), $("word"), $("length"))
//                .execute()
//                .print();

//        tEnv.createFunction("mySplit", MySplit.class);
//        table  // table 和 制表生成的表join在一起 - !!!左外连接!!!
//                .leftOuterJoinLateral(Expressions.call("mySplit", $("id")))  // 把传入的字符串的id的值，做成了表
//                                .select($("id"), $("word"), $("length"))
//                .execute()
//                .print();


        // 2. 在Flink SQL中使用（也必须先注册再使用）
//        tEnv.createFunction("mySplit", MySplit.class);
//        tEnv.createTemporaryView("sensor", table);
//        tEnv
//                .sqlQuery("select " +
//                        " id, word, length " +
////                        " id, w, l " +
//                        " from sensor " +
////                        " join lateral table(mySplit(id)) " +   // - !!!内连接!!!
////                        " as tttttttttttttt(w,l) " +   // ttttttttttttttt是炸出来的临时表名
////                        " on true" +
//                        ", lateral table(mySplit(id))" +
//                        "")
//                .execute()
//                .print();

                tEnv.createFunction("mySplit", MySplit.class);
        tEnv.createTemporaryView("sensor", table);
        tEnv
                .sqlQuery("select " +
                        " id, w, l " +
                        " from sensor " +
                        " left outer join lateral table(mySplit(id)) " +   // - !!!内连接!!!
                        " as tttttttttttttt(w,l) " +   // ttttttttttttttt是炸出来的临时表名
                        " on true" +
                        "")
                .execute()
                .print();

    }

    // Table Function - 实现 制表函数 实现类
    /*
    * ab a c
    *   ab 2
    *   a  1
    *   c  1
    *
    * atguigu aaaa
    *   atguigu  7
    *   aaaa     4
    * */
//    @FunctionHint(output = @DataTypeHint("row<word string, length int>"))  // 由于是弱类型，因此需要使用注解方式，指明Row的名称与类型
//    public static class MySplit extends TableFunction<Row>{
//        public void eval(String str){
//            String[] split = str.split(" ");
//            for (String s : split) {
//                collect(Row.of(s, s.length()));
//            }
//        }
//    }

    public static class MySplit extends TableFunction<WordLen>{
        public void eval(String str){
            if (str.contains("atguigu")) {
                return;
            }
            String[] split = str.split(" +");  // 正则表达式，匹配1个或多个空格
            for (String s : split) {
                collect(new WordLen(s, s.length()));
            }
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WordLen{  // 强类型
        private String word;
        private Integer length;
    }

}


        // 运行结果
        // +----+--------------------------------+--------------------------------+-------------+
        //| op |                             id |                           word |      length |
        //+----+--------------------------------+--------------------------------+-------------+
        //| +I |                         ab a c |                             ab |           2 |
        //| +I |                         ab a c |                              a |           1 |
        //| +I |                         ab a c |                              c |           1 |
        //| +I |                          a b c |                              a |           1 |
        //| +I |                          a b c |                              b |           1 |
        //| +I |                          a b c |                              c |           1 |
        //| +I |                          c d e |                              c |           1 |
        //| +I |                          c d e |                              d |           1 |
        //| +I |                          c d e |                              e |           1 |
        //| +I |                             aa |                             aa |           2 |
        //| +I |                           a ba |                              a |           1 |
        //| +I |                           a ba |                             ba |           2 |
        //| +I |                         ws_002 |                         ws_002 |           6 |
        //+----+--------------------------------+--------------------------------+-------------+

