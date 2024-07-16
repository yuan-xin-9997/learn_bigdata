
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
 * @description: Flink 窗口Window - Flink SQl 中使用窗口 - 使用TVFs实现
 * ref: <a href="https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/window-tvf/#tumble">doc</a>
 * Windows are at the heart of processing infinite streams. Windows split the stream into “buckets” of finite size, over which we can apply computations. This document focuses on how windowing is performed in Flink SQL and how the programmer can benefit to the maximum from its offered functionality.
 *
 * Apache Flink provides several window table-valued functions (TVF) to divide the elements of your table into windows, including:
 *
 * Tumble Windows
 * Hop Windows
 * Cumulate Windows
 * Session Windows (will be supported soon)
 * Note that each element can logically belong to more than one window, depending on the windowing table-valued function you use. For example, HOP windowing creates overlapping windows wherein a single element can be assigned to multiple windows.
 *
 * Windowing TVFs are Flink defined Polymorphic Table Functions (abbreviated PTF). PTF is part of the SQL 2016 standard, a special table-function, but can have a table as a parameter. PTF is a powerful feature to change the shape of a table. Because PTFs are used semantically like tables, their invocation occurs in a FROM clause of a SELECT statement.
 *
 * Windowing TVFs is a replacement of legacy Grouped Window Functions. Windowing TVFs is more SQL standard compliant and more powerful to support complex window-based computations, e.g. Window TopN, Window Join. However, Grouped Window Functions can only support Window Aggregation.
 *
 * See more how to apply further computations based on windowing TVF:
 *
 * Window Aggregation
 * Window TopN
 * Window Join (will be supported soon)
 *
 * Window Functions #
 * Apache Flink provides 3 built-in windowing TVFs: TUMBLE, HOP and CUMULATE. The return value of windowing TVF is a new relation that includes all columns of original relation as well as additional 3 columns named “window_start”, “window_end”, “window_time” to indicate the assigned window. The “window_time” field is a time attributes of the window after windowing TVF which can be used in subsequent time-based operations, e.g. another windowing TVF, or interval joins, over aggregations. The value of window_time always equal to window_end - 1ms.
 *
 * TUMBLE #
 * The TUMBLE function assigns each element to a window of specified window size. Tumbling windows have a fixed size and do not overlap. For example, suppose you specify a tumbling window with a size of 5 minutes. In that case, Flink will evaluate the current window, and a new window started every five minutes, as illustrated by the following figure.
 *
 * Tumbling Windows
 * The TUMBLE function assigns a window for each row of a relation based on a time attribute column. The return value of TUMBLE is a new relation that includes all columns of original relation as well as additional 3 columns named “window_start”, “window_end”, “window_time” to indicate the assigned window. The original time attribute “timecol” will be a regular timestamp column after window TVF.
 *
 * TUMBLE function takes three required parameters:
 *
 * TUMBLE(TABLE data, DESCRIPTOR(timecol), size)
 * data: is a table parameter that can be any relation with a time attribute column.
 * timecol: is a column descriptor indicating which time attributes column of data should be mapped to tumbling windows.
 * size: is a duration specifying the width of the tumbling windows.
 * Here is an example invocation on the Bid table:
 *
 * -- tables must have time attribute, e.g. `bidtime` in this table
 * Flink SQL> desc Bid;
 * +-------------+------------------------+------+-----+--------+---------------------------------+
 * |        name |                   type | null | key | extras |                       watermark |
 * +-------------+------------------------+------+-----+--------+---------------------------------+
 * |     bidtime | TIMESTAMP(3) *ROWTIME* | true |     |        | `bidtime` - INTERVAL '1' SECOND |
 * |       price |         DECIMAL(10, 2) | true |     |        |                                 |
 * |        item |                 STRING | true |     |        |                                 |
 * +-------------+------------------------+------+-----+--------+---------------------------------+
 *
 * Flink SQL> SELECT * FROM Bid;
 * +------------------+-------+------+
 * |          bidtime | price | item |
 * +------------------+-------+------+
 * | 2020-04-15 08:05 |  4.00 | C    |
 * | 2020-04-15 08:07 |  2.00 | A    |
 * | 2020-04-15 08:09 |  5.00 | D    |
 * | 2020-04-15 08:11 |  3.00 | B    |
 * | 2020-04-15 08:13 |  1.00 | E    |
 * | 2020-04-15 08:17 |  6.00 | F    |
 * +------------------+-------+------+
 *
 * -- NOTE: Currently Flink doesn't support evaluating individual window table-valued function,
 * --  window table-valued function should be used with aggregate operation,
 * --  this example is just used for explaining the syntax and the data produced by table-valued function.
 * Flink SQL> SELECT * FROM TABLE(
 *    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES));
 * -- or with the named params
 * -- note: the DATA param must be the first
 * Flink SQL> SELECT * FROM TABLE(
 *    TUMBLE(
 *      DATA => TABLE Bid,
 *      TIMECOL => DESCRIPTOR(bidtime),
 *      SIZE => INTERVAL '10' MINUTES));
 * +------------------+-------+------+------------------+------------------+-------------------------+
 * |          bidtime | price | item |     window_start |       window_end |            window_time  |
 * +------------------+-------+------+------------------+------------------+-------------------------+
 * | 2020-04-15 08:05 |  4.00 | C    | 2020-04-15 08:00 | 2020-04-15 08:10 | 2020-04-15 08:09:59.999 |
 * | 2020-04-15 08:07 |  2.00 | A    | 2020-04-15 08:00 | 2020-04-15 08:10 | 2020-04-15 08:09:59.999 |
 * | 2020-04-15 08:09 |  5.00 | D    | 2020-04-15 08:00 | 2020-04-15 08:10 | 2020-04-15 08:09:59.999 |
 * | 2020-04-15 08:11 |  3.00 | B    | 2020-04-15 08:10 | 2020-04-15 08:20 | 2020-04-15 08:19:59.999 |
 * | 2020-04-15 08:13 |  1.00 | E    | 2020-04-15 08:10 | 2020-04-15 08:20 | 2020-04-15 08:19:59.999 |
 * | 2020-04-15 08:17 |  6.00 | F    | 2020-04-15 08:10 | 2020-04-15 08:20 | 2020-04-15 08:19:59.999 |
 * +------------------+-------+------+------------------+------------------+-------------------------+
 *
 * -- apply aggregation on the tumbling windowed table
 * Flink SQL> SELECT window_start, window_end, SUM(price)
 *   FROM TABLE(
 *     TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
 *   GROUP BY window_start, window_end;
 * +------------------+------------------+-------+
 * |     window_start |       window_end | price |
 * +------------------+------------------+-------+
 * | 2020-04-15 08:00 | 2020-04-15 08:10 | 11.00 |
 * | 2020-04-15 08:10 | 2020-04-15 08:20 | 10.00 |
 * +------------------+------------------+-------+
 * Note: in order to better understand the behavior of windowing, we simplify the displaying of timestamp values to not show the trailing zeros, e.g. 2020-04-15 08:05 should be displayed as 2020-04-15 08:05:00.000 in Flink SQL Client if the type is TIMESTAMP(3).
 *
 * HOP #
 * The HOP function assigns elements to windows of fixed length. Like a TUMBLE windowing function, the size of the windows is configured by the window size parameter. An additional window slide parameter controls how frequently a hopping window is started. Hence, hopping windows can be overlapping if the slide is smaller than the window size. In this case, elements are assigned to multiple windows. Hopping windows are also known as “sliding windows”.
 *
 * For example, you could have windows of size 10 minutes that slides by 5 minutes. With this, you get every 5 minutes a window that contains the events that arrived during the last 10 minutes, as depicted by the following figure.
 *
 * Hopping windows
 * The HOP function assigns windows that cover rows within the interval of size and shifting every slide based on a time attribute column. The return value of HOP is a new relation that includes all columns of original relation as well as additional 3 columns named “window_start”, “window_end”, “window_time” to indicate the assigned window. The original time attribute “timecol” will be a regular timestamp column after windowing TVF.
 *
 * HOP takes three required parameters.
 *
 * HOP(TABLE data, DESCRIPTOR(timecol), slide, size [, offset ])
 * data: is a table parameter that can be any relation with an time attribute column.
 * timecol: is a column descriptor indicating which time attributes column of data should be mapped to hopping windows.
 * slide: is a duration specifying the duration between the start of sequential hopping windows
 * size: is a duration specifying the width of the hopping windows.
 * Here is an example invocation on the Bid table:
 *
 * -- NOTE: Currently Flink doesn't support evaluating individual window table-valued function,
 * --  window table-valued function should be used with aggregate operation,
 * --  this example is just used for explaining the syntax and the data produced by table-valued function.
 * > SELECT * FROM TABLE(
 *     HOP(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES));
 * -- or with the named params
 * -- note: the DATA param must be the first
 * > SELECT * FROM TABLE(
 *     HOP(
 *       DATA => TABLE Bid,
 *       TIMECOL => DESCRIPTOR(bidtime),
 *       SLIDE => INTERVAL '5' MINUTES,
 *       SIZE => INTERVAL '10' MINUTES));
 * +------------------+-------+------+------------------+------------------+-------------------------+
 * |          bidtime | price | item |     window_start |       window_end |           window_time   |
 * +------------------+-------+------+------------------+------------------+-------------------------+
 * | 2020-04-15 08:05 |  4.00 | C    | 2020-04-15 08:00 | 2020-04-15 08:10 | 2020-04-15 08:09:59.999 |
 * | 2020-04-15 08:05 |  4.00 | C    | 2020-04-15 08:05 | 2020-04-15 08:15 | 2020-04-15 08:14:59.999 |
 * | 2020-04-15 08:07 |  2.00 | A    | 2020-04-15 08:00 | 2020-04-15 08:10 | 2020-04-15 08:09:59.999 |
 * | 2020-04-15 08:07 |  2.00 | A    | 2020-04-15 08:05 | 2020-04-15 08:15 | 2020-04-15 08:14:59.999 |
 * | 2020-04-15 08:09 |  5.00 | D    | 2020-04-15 08:00 | 2020-04-15 08:10 | 2020-04-15 08:09:59.999 |
 * | 2020-04-15 08:09 |  5.00 | D    | 2020-04-15 08:05 | 2020-04-15 08:15 | 2020-04-15 08:14:59.999 |
 * | 2020-04-15 08:11 |  3.00 | B    | 2020-04-15 08:05 | 2020-04-15 08:15 | 2020-04-15 08:14:59.999 |
 * | 2020-04-15 08:11 |  3.00 | B    | 2020-04-15 08:10 | 2020-04-15 08:20 | 2020-04-15 08:19:59.999 |
 * | 2020-04-15 08:13 |  1.00 | E    | 2020-04-15 08:05 | 2020-04-15 08:15 | 2020-04-15 08:14:59.999 |
 * | 2020-04-15 08:13 |  1.00 | E    | 2020-04-15 08:10 | 2020-04-15 08:20 | 2020-04-15 08:19:59.999 |
 * | 2020-04-15 08:17 |  6.00 | F    | 2020-04-15 08:10 | 2020-04-15 08:20 | 2020-04-15 08:19:59.999 |
 * | 2020-04-15 08:17 |  6.00 | F    | 2020-04-15 08:15 | 2020-04-15 08:25 | 2020-04-15 08:24:59.999 |
 * +------------------+-------+------+------------------+------------------+-------------------------+
 *
 * -- apply aggregation on the hopping windowed table
 * > SELECT window_start, window_end, SUM(price)
 *   FROM TABLE(
 *     HOP(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES))
 *   GROUP BY window_start, window_end;
 * +------------------+------------------+-------+
 * |     window_start |       window_end | price |
 * +------------------+------------------+-------+
 * | 2020-04-15 08:00 | 2020-04-15 08:10 | 11.00 |
 * | 2020-04-15 08:05 | 2020-04-15 08:15 | 15.00 |
 * | 2020-04-15 08:10 | 2020-04-15 08:20 | 10.00 |
 * | 2020-04-15 08:15 | 2020-04-15 08:25 |  6.00 |
 * +------------------+------------------+-------+
 * CUMULATE #
 * Cumulating windows are very useful in some scenarios, such as tumbling windows with early firing in a fixed window interval. For example, a daily dashboard draws cumulative UVs from 00:00 to every minute, the UV at 10:00 represents the total number of UV from 00:00 to 10:00. This can be easily and efficiently implemented by CUMULATE windowing.
 *
 * The CUMULATE function assigns elements to windows that cover rows within an initial interval of step size and expand to one more step size (keep window start fixed) every step until the max window size. You can think CUMULATE function as applying TUMBLE windowing with max window size first, and split each tumbling windows into several windows with same window start and window ends of step-size difference. So cumulating windows do overlap and don’t have a fixed size.
 *
 * For example, you could have a cumulating window for 1 hour step and 1 day max size, and you will get windows: [00:00, 01:00), [00:00, 02:00), [00:00, 03:00), …, [00:00, 24:00) for every day.
 *
 * Cumulating Windows
 * The CUMULATE functions assigns windows based on a time attribute column. The return value of CUMULATE is a new relation that includes all columns of original relation as well as additional 3 columns named “window_start”, “window_end”, “window_time” to indicate the assigned window. The original time attribute “timecol” will be a regular timestamp column after window TVF.
 *
 * CUMULATE takes three required parameters.
 *
 * CUMULATE(TABLE data, DESCRIPTOR(timecol), step, size)
 * data: is a table parameter that can be any relation with an time attribute column.
 * timecol: is a column descriptor indicating which time attributes column of data should be mapped to tumbling windows.
 * step: is a duration specifying the increased window size between the end of sequential cumulating windows.
 * size: is a duration specifying the max width of the cumulating windows. size must be an integral multiple of step .
 * Here is an example invocation on the Bid table:
 *
 * -- NOTE: Currently Flink doesn't support evaluating individual window table-valued function,
 * --  window table-valued function should be used with aggregate operation,
 * --  this example is just used for explaining the syntax and the data produced by table-valued function.
 * > SELECT * FROM TABLE(
 *     CUMULATE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES));
 * -- or with the named params
 * -- note: the DATA param must be the first
 * > SELECT * FROM TABLE(
 *     CUMULATE(
 *       DATA => TABLE Bid,
 *       TIMECOL => DESCRIPTOR(bidtime),
 *       STEP => INTERVAL '2' MINUTES,
 *       SIZE => INTERVAL '10' MINUTES));
 * +------------------+-------+------+------------------+------------------+-------------------------+
 * |          bidtime | price | item |     window_start |       window_end |            window_time  |
 * +------------------+-------+------+------------------+------------------+-------------------------+
 * | 2020-04-15 08:05 |  4.00 | C    | 2020-04-15 08:00 | 2020-04-15 08:06 | 2020-04-15 08:05:59.999 |
 * | 2020-04-15 08:05 |  4.00 | C    | 2020-04-15 08:00 | 2020-04-15 08:08 | 2020-04-15 08:07:59.999 |
 * | 2020-04-15 08:05 |  4.00 | C    | 2020-04-15 08:00 | 2020-04-15 08:10 | 2020-04-15 08:09:59.999 |
 * | 2020-04-15 08:07 |  2.00 | A    | 2020-04-15 08:00 | 2020-04-15 08:08 | 2020-04-15 08:07:59.999 |
 * | 2020-04-15 08:07 |  2.00 | A    | 2020-04-15 08:00 | 2020-04-15 08:10 | 2020-04-15 08:09:59.999 |
 * | 2020-04-15 08:09 |  5.00 | D    | 2020-04-15 08:00 | 2020-04-15 08:10 | 2020-04-15 08:09:59.999 |
 * | 2020-04-15 08:11 |  3.00 | B    | 2020-04-15 08:10 | 2020-04-15 08:12 | 2020-04-15 08:11:59.999 |
 * | 2020-04-15 08:11 |  3.00 | B    | 2020-04-15 08:10 | 2020-04-15 08:14 | 2020-04-15 08:13:59.999 |
 * | 2020-04-15 08:11 |  3.00 | B    | 2020-04-15 08:10 | 2020-04-15 08:16 | 2020-04-15 08:15:59.999 |
 * | 2020-04-15 08:11 |  3.00 | B    | 2020-04-15 08:10 | 2020-04-15 08:18 | 2020-04-15 08:17:59.999 |
 * | 2020-04-15 08:11 |  3.00 | B    | 2020-04-15 08:10 | 2020-04-15 08:20 | 2020-04-15 08:19:59.999 |
 * | 2020-04-15 08:13 |  1.00 | E    | 2020-04-15 08:10 | 2020-04-15 08:14 | 2020-04-15 08:13:59.999 |
 * | 2020-04-15 08:13 |  1.00 | E    | 2020-04-15 08:10 | 2020-04-15 08:16 | 2020-04-15 08:15:59.999 |
 * | 2020-04-15 08:13 |  1.00 | E    | 2020-04-15 08:10 | 2020-04-15 08:18 | 2020-04-15 08:17:59.999 |
 * | 2020-04-15 08:13 |  1.00 | E    | 2020-04-15 08:10 | 2020-04-15 08:20 | 2020-04-15 08:19:59.999 |
 * | 2020-04-15 08:17 |  6.00 | F    | 2020-04-15 08:10 | 2020-04-15 08:18 | 2020-04-15 08:17:59.999 |
 * | 2020-04-15 08:17 |  6.00 | F    | 2020-04-15 08:10 | 2020-04-15 08:20 | 2020-04-15 08:19:59.999 |
 * +------------------+-------+------+------------------+------------------+-------------------------+
 *
 * -- apply aggregation on the cumulating windowed table
 * > SELECT window_start, window_end, SUM(price)
 *   FROM TABLE(
 *     CUMULATE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES))
 *   GROUP BY window_start, window_end;
 * +------------------+------------------+-------+
 * |     window_start |       window_end | price |
 * +------------------+------------------+-------+
 * | 2020-04-15 08:00 | 2020-04-15 08:06 |  4.00 |
 * | 2020-04-15 08:00 | 2020-04-15 08:08 |  6.00 |
 * | 2020-04-15 08:00 | 2020-04-15 08:10 | 11.00 |
 * | 2020-04-15 08:10 | 2020-04-15 08:12 |  3.00 |
 * | 2020-04-15 08:10 | 2020-04-15 08:14 |  4.00 |
 * | 2020-04-15 08:10 | 2020-04-15 08:16 |  4.00 |
 * | 2020-04-15 08:10 | 2020-04-15 08:18 | 10.00 |
 * | 2020-04-15 08:10 | 2020-04-15 08:20 | 10.00 |
 * +------------------+------------------+-------+
 *
 */
public class FIink02_Window_TVFs_1 {
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
                new WaterSensor("ws_002", 6001L, 60),
                new WaterSensor("ws_002", 9001L, 60),
                new WaterSensor("ws_002", 10001L, 60),
                new WaterSensor("ws_002", 15001L, 60),
                new WaterSensor("ws_002", 16001L, 60),
                new WaterSensor("ws_002", 20001L, 60),
                new WaterSensor("ws_002", 25001L, 60)
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

        // TVFs - 滚动窗口
//        tEnv
//                .sqlQuery("select " +
//                        " id, window_start, window_end " +
//                        " , sum(vc) vc_sum" +
//                        //" from table( tumble(table sensor, descriptor(et), interval '5' seconds) )" +
//                        " from table( tumble(DATA => table sensor, TIMECOL => descriptor(et), SIZE => interval '5' seconds) )" +
//                        " group by id, window_start, window_end")
//                .execute()
//                .print()
//                ;

        // TVFs - 滑动窗口
        // HOP table function based aggregate requires size must be an integral multiple of slide, but got size 5000 ms and slide 2000 ms
        //
//        tEnv
//                .sqlQuery("select " +
//                        " id, window_start, window_end " +
//                        " , sum(vc) vc_sum" +
//                        " from table( hop(table sensor, descriptor(et), interval '2.5' seconds, interval '5' seconds) )" +
//                        " group by id, window_start, window_end")
//                .execute()
//                .print()
//                ;

        // TVFs - 累积窗口
        tEnv
                .sqlQuery("select " +
                        " id, window_start, window_end " +
                        " , sum(vc) vc_sum" +
                        " from table( cumulate(table sensor, descriptor(et), interval '5' seconds, interval '20' seconds) )" +
                        " group by id, window_start, window_end")
                .execute()
                .print()
                ;
    }
}
