package com.atguigu.chapter05_sink;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/13 21:10
 * @contact: yuanxin9997@qq.com
 * @description: Flink04_Custom_MySQL_sink 自定义Sink
 */
public class Flink05_JDBC_sink {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 160924000002L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 160924000004L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 160924000006L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 160924000003L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 160924000005L, 30));
        DataStreamSource<WaterSensor> waterSensorDS = env.fromCollection(waterSensors);
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.keyBy(WaterSensor::getId)
                .sum("vc");
        
        // 写入到MySQL
        result.addSink(
                JdbcSink.sink(
                        "replace into sensor(id,ts,vc) values(?,?,?)",
                        new JdbcStatementBuilder<WaterSensor>() {
                            @Override
                            public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                                // 只做一件事，给占位符赋值
                                // 千万注意，这里不能关闭prepareStatement，因为会做重用
                                preparedStatement.setString(1, waterSensor.getId());
                                preparedStatement.setLong(2, waterSensor.getTs());
                                preparedStatement.setInt(3, waterSensor.getVc());
                            }
                        },
                        new JdbcExecutionOptions.Builder()  // 执行参数
                                .withBatchSize(1024)   // Bytes  刷新大小
                                .withBatchIntervalMs(2000)  // 毫秒  刷新间隔
                                .withMaxRetries(3)  // 最大重试次数
                                .build(),  // 设置执行参数
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()   // 连接参数
                                .withDriverName("com.mysql.jdbc.Driver")  // 驱动类名
                                .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")  // 数据库URL
                                .withUsername("root")  // 用户名
                                .withPassword("root")
                                .build()
                )
        );

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
