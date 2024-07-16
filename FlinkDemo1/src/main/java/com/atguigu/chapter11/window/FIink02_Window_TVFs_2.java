
package com.atguigu.chapter11.window;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/16 18:43
 * @contact: yuanxin9997@qq.com
 * @description: Flink 窗口Window - Flink SQl 中使用窗口 - 使用TVFs实现窗口 分组集 Group Set
 * ref: <a href="https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/window-tvf/#tumble">doc</a>
 */
public class FIink02_Window_TVFs_2 {
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
        // 创建表
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        table.printSchema();
        //table.execute().print();

        // 注册临时表
        tEnv.createTemporaryView("sensor", table);

        // TVFs - 不使用分组集
//        tEnv
//                .sqlQuery("select " +
//                        " window_start, window_end, id " +
//                        " , sum(vc) vc_sum" +
//                        " from table( tumble(table sensor, descriptor(et), interval '5' seconds) )" +
//                        " group by id, window_start, window_end" +
//                        "  union " +
//                        "select " +
//                        " window_start, window_end, 'a' id " +
//                        " , sum(vc) vc_sum" +
//                        " from table( tumble(table sensor, descriptor(et), interval '5' seconds) )" +
//                        " group by window_start, window_end"
//                )
//                .execute()
//                .print()
//        ;

        // TVFs - 使用分组集 Grouping sets
        // 上钻  grouping sets( (a,b,c), (a,b), (a), () )  == rollup(a,b,c)
        // cube(a,b,c) = a b c的组合，2的3次方=8
        tEnv
                .sqlQuery("select " +
                        " window_start, window_end, id " +
                        " , sum(vc) vc_sum" +
                        " from table( tumble(table sensor, descriptor(et), interval '5' seconds) )" +
                        //" group by window_start, window_end, grouping sets((id),())"
                        //" group by window_start, window_end,  rollup(id) "
                        " group by window_start, window_end,  cube(id) "
                )
                .execute()
                .print()
        ;


    }
}
