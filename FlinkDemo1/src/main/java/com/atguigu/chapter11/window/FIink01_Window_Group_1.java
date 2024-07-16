package com.atguigu.chapter11.window;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/16 18:43
 * @contact: yuanxin9997@qq.com
 * @description: Flink 窗口Window - Table API中使用窗口
 *
 * 11.5窗口(window)
 * 时间语义，要配合窗口操作才能发挥作用。最主要的用途，当然就是开窗口然后根据时间段做计算了。
 * 下面我们就来看看Table API和SQL中，怎么利用时间字段做窗口操作。
 * 在Table API和SQL中，主要有两种窗口：Group Windows和Over Windows。
 *
 * 11.5.1Table API中使用窗口
 * Group Windows
 * 分组窗口（Group Windows）会根据时间或行计数间隔，将行聚合到有限的组（Group）中，并对每个组的数据执行一次聚合函数。
 * Table API中的Group Windows都是使用.window（w:GroupWindow）子句定义的，并且必须由as子句指定一
 * 个别名。为了按窗口对表进行分组，窗口的别名必须在group by子句中，像常规的分组字段一样引用。
 */
public class FIink01_Window_Group_1 {
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
                new WaterSensor("ws_002", 2000L, 20),
                new WaterSensor("ws_001", 3000L, 30),
                new WaterSensor("ws_002", 4000L, 40),
                new WaterSensor("ws_001", 5000L, 50),
                new WaterSensor("ws_002", 6001L, 60)
         )
                 .assignTimestampsAndWatermarks(
                         WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                 .withTimestampAssigner( (value, timestamp) -> value.getTs())
                 );
        // 创建流表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 创建表
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        table.printSchema();
        //table.execute().print();

        // 用table api方式实现窗口   lit = literal 字面量 比如 1 2 "abc"
        //GroupWindow window = Tumble.over(Expressions.lit(5).second()).on($("et")).as("w");  // 滚动窗口
//        GroupWindow window = Slide.over(
//                Expressions.lit(5).second()
//        ).every(
//                Expressions.lit(2).second()
//        ).on($("et")).as("w");  // 滑动窗口
        GroupWindow window = Session.withGap(
                Expressions.lit(2).second()
        ).on($("et")).as("w");  // 会话窗口

        table
                .window(window)
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").start(), $("w").end(), $("vc").sum().as("vc_sum"))
//                .groupBy( $("w"))
//                .select($("w").start(), $("w").end(), $("vc").sum().as("vc_sum"))
                .execute()
                .print();

        // 滚动窗口执行结果：
        // +----+-------------------------+-------------------------+-------------+
        //| op |                  EXPR$0 |                  EXPR$1 |      vc_sum |
        //+----+-------------------------+-------------------------+-------------+
        //| +I | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |         100 |
        //| +I | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |         110 |
        //+----+-------------------------+-------------------------+-------------+
        //2 rows in set
    }
}
