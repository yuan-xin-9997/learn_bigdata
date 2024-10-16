package com.atguigu.chapter11.functions;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/17 19:40
 * @contact: yuanxin9997@qq.com
 * @description: Flink 自定义函数UDF -  Table aggregate 函数 - Table aggregate functions map scalar values of multiple rows to new rows.
 * 此处TableAggregate Function，即制表聚合函数，进一行，出多行，并在转换过程中进行聚合
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
public class Flink04_function_TableAggregate {
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
                        new WaterSensor("atguigu ab", 6001L, 60),
                        new WaterSensor("atguigu ab", 6001L, 80)
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


        //  需求：每来1个水位，输出第一名和第二名（使用TableAggregate函数实现）
        /*
        * new WaterSensor("s1",1000L,10)
        *   第一名10
        * newWaterSensor("s1",2000L，20)
        *   第一名20
        *   第二名10
        * newWaterSensor("s2",3000L,30)
        *   第一名30
        *   第二名20
        * */

        // 自定义UDF函数的使用方式
        // 1. 在TableAPI中使用
        // 1.1 内联的方式使用

        // 1.2 先注册，再使用
        tEnv.createFunction("top2", Top2.class);
        table  // table
                .groupBy($("id"))
                .flatAggregate(Expressions.call("top2", $("vc")))
                .select($("id"), $("rank"), $("vc"))
                .execute()
                .print();

        // 2. 在Flink SQL中使用（也必须先注册再使用）
        // 不支持
    }

    // TableAggregate Function - 制表聚合函数 实现类
    public static class Top2 extends TableAggregateFunction<Result, FirstSecond>{

        // 初始化累加器
        @Override
        public FirstSecond createAccumulator() {
            return new FirstSecond();
        }

        // 累加方法，约定方法，返回值必须是void，参数1必须是累加器，参数2根据实际情况来定，call中传入的参数
        public void accumulate(FirstSecond fs, Integer vc){
            if(vc > fs.first){
                fs.second = fs.first;
                fs.first = vc;
            }else if(vc > fs.second){
                fs.second = vc;
            }
        }

        // 获取累加结果 ， 发射数据
        public void emitValue(FirstSecond fs, Collector<Result> out){
            out.collect(new Result("第一名", fs.first));
            if (fs.second>Integer.MIN_VALUE) {
                out.collect(new Result("第二名", fs.second));
            }
        }
    }

    public static class Result{
        public String rank;
        public Integer vc;

        public Result(String rank, Integer vc) {
            this.rank = rank;
            this.vc = vc;
        }
    }

    public static class FirstSecond{
        public Integer first = Integer.MIN_VALUE;
        public Integer second = Integer.MIN_VALUE;
    }

}