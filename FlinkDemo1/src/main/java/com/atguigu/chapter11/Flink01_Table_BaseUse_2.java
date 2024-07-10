package com.atguigu.chapter11;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/10 20:13
 * @contact: yuanxin9997@qq.com
 * @description: Flink Table API 11.2.3基本使用 聚合操作
 *
 * 11.2.4表到流的转换
 * 动态表可以像普通数据库表一样通过 INSERT、UPDATE 和 DELETE 来不断修改。它可能是一个只有一行、不断更新的表，也可能是一个 insert-only 的表，没有 UPDATE 和 DELETE 修改，或者介于两者之间的其他表。
 * 在将动态表转换为流或将其写入外部系统时，需要对这些更改进行编码。Flink的 Table API 和 SQL 支持三种方式来编码一个动态表的变化:
 * Append-only 流
 * 仅通过INSERT操作修改的动态表可以通过输出插入的行转换为流。
 * Retract 流
 * retract 流包含两种类型的 message：add messages和retract messages。通过将INSERT操作编码为 add message、将DELETE操作编码为 retract message、将UPDATE操作编码为更新(先前)行的 retract message 和更新(新)行的 add message，将动态表转换为 retract 流。下图显示了将动态表转换为 retract 流的过程。
 *
 * Upsert 流
 * upsert 流包含两种类型的 message：upsert messages和delete messages。转换为 upsert 流的动态表需要(可能是组合的)唯一键。通过将INSERT和UPDATE操作编码为 upsert message，将DELETE操作编码为 delete message ，将具有唯一键的动态表转换为流。消费流的算子需要知道唯一键的属性，以便正确地应用 message。与 retract 流的主要区别在于UPDATE操作是用单个 message 编码的，因此效率更高。下图显示了将动态表转换为 upsert 流的过程。
 *
 * 请注意，在将动态表转换为DataStream时，只支持 append 流和 retract 流。
 */
public class Flink01_Table_BaseUse_2 {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("ws_001", 1000L, 10),
                new WaterSensor("ws_002", 2000L, 20),
                new WaterSensor("ws_001", 3000L, 30),
                new WaterSensor("ws_002", 4000L, 40),
                new WaterSensor("ws_001", 5000L, 50),
                new WaterSensor("ws_002", 6000L, 60)
        );
        // 2. 创建表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        // 3. 将流转换为表
        Table table = streamTableEnvironment.fromDataStream(stream);
        table.printSchema();

        // 4. 在表上执行查询操作，得到一个新的动态表
        // select * from table where id='ws_001';
        // Table result = table
                //.where("id='ws_001'")
                //.where($("id").isEqual("ws_001"))
//                .select("id,ts,vc")
                //.select("id,ts")
                //.select(Expressions.$("id"), Expressions.$("ts"), Expressions.$("vc"))
                //.select($("id"), $("ts"), $("vc"))
//                ;

        // select id,sum(vc) as vc_sum from table group by id;
//        Table result = table
//                .groupBy($("id"))
//                .aggregate($("vc").sum().as("vc_sum"))
//                .select($("id"), $("vc_sum"))
        Table result = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum().as("vc_sum"))
                ;

        // 5. 将动态表转换为流
        // DataStream<WaterSensor> resultStream = streamTableEnvironment.toDataStream(result, WaterSensor.class);
        // DataStream<Row> resultStream = streamTableEnvironment.toDataStream(result, Row.class);  // Table sink 'default_catalog.default_database.Unregistered_DataStream_Sink_1' doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[id], select=[id, SUM(vc) AS TMP_0])
        // DataStream<Row> resultStream = streamTableEnvironment.toAppendStream(result, Row.class);  // toAppendStream doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[id], select=[id, SUM(vc) AS TMP_0])
        //DataStream<Tuple2<Boolean, Row>> resultStream = streamTableEnvironment.toRetractStream(result, Row.class);
        SingleOutputStreamOperator<Row> resultStream = streamTableEnvironment
                .toRetractStream(result, Row.class)
                .filter(t -> t.f0)
                .map(t -> t.f1);

        // 6. 将流打印输出
        resultStream.print();

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
