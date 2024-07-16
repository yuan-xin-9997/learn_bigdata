package com.atguigu.chapter11.window;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/16 18:43
 * @contact: yuanxin9997@qq.com
 * @description: Flink 窗口Window - Flink SQl 中使用窗口 - 分组窗口
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
 *
 * ref: <a href="https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/window-agg/">参考doc</a>
 *
 *
 *
 * 分组集 Group Set 目前Group Window不支持
 * select a,b,c,sum（vc) from t groupby a,b,c
 * union
 * selecta,b,null
 * sum(vc）from t
 * groupby a,b
 *
 * select
 * a,b,c
 * sum(...)
 * group by grouping set((a,b,c),(a,b))
 */
public class FIink01_Window_Group_2 {
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

        // 注册临时表
        tEnv.createTemporaryView("sensor", table);

        // 滚动窗口
//        tEnv.sqlQuery("select " +
//                " id," +
//                " TUMBLE_START(et, INTERVAL '5' SECOND) stt, " +
//                " TUMBLE_END(et, INTERVAL '5' SECOND) edt, " +
//                " sum(vc) vc_sum" +
//                " from sensor " +
//                " group by id, TUMBLE(et, INTERVAL '5' SECOND)")
//                .execute()
//                .print();
//                ;

        // 滑动窗口
//        tEnv.sqlQuery("select " +
//                " id," +
//                " HOP_START(et, INTERVAL '2' SECOND, INTERVAL '5' SECOND) stt, " +
//                " HOP_END(et, INTERVAL '2' SECOND, INTERVAL '5' SECOND) edt, " +
//                " sum(vc) vc_sum" +
//                " from sensor " +
//                " group by id, HOP(et, INTERVAL '2' SECOND, INTERVAL '5' SECOND)")
//                .execute()
//                .print();

        // 会话窗口
        tEnv.sqlQuery("select " +
                " id," +
                " SESSION_START(et, INTERVAL '2' SECOND) stt, " +
                " SESSION_END(et, INTERVAL '2' SECOND) edt, " +
                " sum(vc) vc_sum" +
                " from sensor " +
                " group by id, SESSION(et, INTERVAL '2' SECOND) ")
                .execute()
                .print();
    }
}
