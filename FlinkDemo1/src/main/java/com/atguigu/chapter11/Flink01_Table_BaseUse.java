package com.atguigu.chapter11;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/10 20:13
 * @contact: yuanxin9997@qq.com
 * @description: Flink Table API 11.2.3基本使用 基本使用:表与DataStream的混合使用
 */
public class Flink01_Table_BaseUse {
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
        Table result = table
                .where("id='ws_001'")
//                .select("id,ts,vc")
                .select("id,ts")
                ;

        // 5. 将动态表转换为流
        // DataStream<WaterSensor> resultStream = streamTableEnvironment.toDataStream(result, WaterSensor.class);
        // DataStream<Row> resultStream = streamTableEnvironment.toDataStream(result, Row.class);
        DataStream<Row> resultStream = streamTableEnvironment.toAppendStream(result, Row.class);

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
