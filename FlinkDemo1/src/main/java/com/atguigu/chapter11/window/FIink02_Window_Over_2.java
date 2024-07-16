
package com.atguigu.chapter11.window;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/16 18:43
 * @contact: yuanxin9997@qq.com
 * @description: Flink 窗口Window - Flink SQl 中使用窗口 - Over窗口
 * Over Windows
 * Over window聚合是标准SQL中已有的（Over子句），可以在查询的SELECT子句中定义。Over window 聚合，会针对每个输入行，计算相邻行范围内的聚合。
 * Table API提供了Over类，来配置Over窗口的属性。可以在事件时间或处理时间，以及指定为时间间隔、或行计数的范围内，定义Over windows。
 * 无界的over window是使用常量指定的。也就是说，时间间隔要指定UNBOUNDED_RANGE，或者行计数间隔要指定UNBOUNDED_ROW。而有界的over window是用间隔的大小指定的。
 * Unbounded Over Windows
 * ounded Over Windows
 */
public class FIink02_Window_Over_2 {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(3);

        // Flink程序主逻辑
        // 先添加水印
        DataStream<WaterSensor> stream = env.fromElements(
                        new WaterSensor("ws_001", 1000L, 10),
                        new WaterSensor("ws_001", 2000L, 20),
                        new WaterSensor("ws_001", 3000L, 30),
                        new WaterSensor("ws_001", 3000L, 40),
                        new WaterSensor("ws_001", 5000L, 50),
                        new WaterSensor("ws_001", 6001L, 60)
                        ,new WaterSensor("ws_002", 9001L, 60),
                        new WaterSensor("ws_002", 10001L, 60),
                        new WaterSensor("ws_002", 15001L, 60),
                        new WaterSensor("ws_002", 16001L, 60),
                        new WaterSensor("ws_002", 20001L, 60),
                        new WaterSensor("ws_002", 25001L, 60)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((value, timestamp) -> value.getTs())
                );
        // 创建流表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 创建表
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        table.printSchema();
        //table.execute().print();

        // 注册临时表
        tEnv.createTemporaryView("sensor", table);

        // Flink SQL Over 窗口
//        tEnv
//                .sqlQuery("select " +
//                        " id,ts,vc,et," +
//                        //" sum(vc) over(partition by id order by et rows between unbounded preceding and current row) vc_sum" +
//                        //" sum(vc) over(partition by id order by et rows between 1 preceding and current row) vc_sum" +
//                        // " sum(vc) over(partition by id order by et range between unbounded preceding and current row) vc_sum" +
//                        // " sum(vc) over(partition by id order by et range between interval '1' second preceding and current row) vc_sum" +
//                        " sum(vc) over(partition by id order by et) vc_sum" +  // = rows between unbounded preceding and current row
//                        " from sensor")
//                .execute()
//                .print();

        // 多个Over窗口
        tEnv
                .sqlQuery("select " +
                        " id,ts,vc,et," +
                        " sum(vc) over w vc_sum, " +
                        " max(vc) over w vc_max" +
                        " from sensor" +
                        " window w as(partition by id order by et rows between unbounded preceding and current row) "
                        )
                .execute()
                .print();

    }
}
