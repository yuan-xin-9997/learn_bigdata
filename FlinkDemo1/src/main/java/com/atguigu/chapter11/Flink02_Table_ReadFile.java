package com.atguigu.chapter11;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;


import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/10 20:13
 * @contact: yuanxin9997@qq.com
 * @description: Flink Table API
 */
public class Flink02_Table_ReadFile {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        // 2. 创建表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        // Table API 读取文件，建立到文件的连接
        // 数据会自动进入到一个叫做sensor的表中
        streamTableEnvironment
                .connect(new FileSystem().path("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\sensor.txt"))
                .withFormat(new Csv())  // 行：\n  列：,
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("ts", DataTypes.BIGINT())
                                .field("vc", DataTypes.INT())
                )
                .createTemporaryTable("sensor");

        // 得到一个table对象
        Table table = streamTableEnvironment.from("sensor");
        table.execute().print();

        // 编写SQL、执行SQL
//        table
//                .where($("id").isEqual("sensor_1"))
//                .select($("id"), $("ts"), $("vc"))
//                .execute()
//                .print();

        // 把SQL执行结果写入到文件中
        // 创建一个动态表，并与文件进行关联
        streamTableEnvironment
                .connect(new FileSystem().path("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\a.log"))
                .withFormat(new Csv())  // 行：\n  列：,
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("ts", DataTypes.BIGINT())
                                .field("vc", DataTypes.INT())
                )
                .createTemporaryTable("abc");
        table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"))
                .executeInsert("abc");  // 将SQL查询结果写入到表中，相当于写入到文件中

    }
}
