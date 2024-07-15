package com.atguigu.chapter11;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月15日18:40:03
 * @contact: yuanxin9997@qq.com
 * @description: Flink Table API
 */
public class Flink03_Table_ReadKafka {
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

        // Table API Connector - Kafka
        streamTableEnvironment
                .connect(
                        //new FileSystem().path("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\sensor.txt")
                        new Kafka().version("universal")
                                .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
                                .topic("s1")
                                .property("group.id", "atguigu")
                                .startFromLatest()
                )
                .withFormat(
                        //new Csv()
                        new Json()
                )  // 行：\n  列：, 读取Kafka的解析格式
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("ts", DataTypes.BIGINT())
                                .field("vc", DataTypes.INT())
                )
                .createTemporaryTable("sensor");

        // 得到一个table对象
        Table table = streamTableEnvironment.from("sensor").where($("vc").isGreater(10))
                .select("*");
        //table.execute().print();  // 注意：！！！如果执行了，下面executeInsert则无法执行

        // 创建一个动态表，并与输出Kafka进行关联
        streamTableEnvironment
                .connect(
                        new Kafka().version("universal")
                                .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
                                .topic("s2")
                )
                .withFormat(new Json())  // 行：\n  列：,
                .withSchema(
                                new Schema()
                                .field("id", DataTypes.STRING())
                                .field("ts", DataTypes.BIGINT())
                                .field("vc", DataTypes.INT())
                )
                .createTemporaryTable("abc");
        // 把SQL执行结果写入到Kafka中
        table.executeInsert("abc");  // 将SQL查询结果写入到表中，相当于写入到文件中
    }
}
