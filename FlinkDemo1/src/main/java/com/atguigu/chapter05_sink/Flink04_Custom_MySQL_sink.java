package com.atguigu.chapter05_sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/13 21:10
 * @contact: yuanxin9997@qq.com
 * @description: Flink04_Custom_MySQL_sink 自定义Sink
 */
public class Flink04_Custom_MySQL_sink {
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
        result.addSink(new MySQLSink());

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static class MySQLSink extends RichSinkFunction<WaterSensor> {

        private Connection connection;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 建立到MySQL的连接
            // 1. 加载驱动
            Class.forName("com.mysql.jdbc.Driver");
            // 2. 建立连接      IDEA 快捷键 ctrl + alt + f 提升成员变量
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "root");
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        // 调用：每来1条元素，这个方法执行一次
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            // JDBC的方式向MySQL写数据
            // 如果不重复，就insert，如果主键重复，则update vc
            // String sql = "insert into sensor(id,ts,vc) values(?,?,?) on duplicate key update vc=?";
            String sql = "replace into sensor(id,ts,vc) values(?,?,?)";
            // 得到预处理语句
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            // 给SQL中占位符进行赋值
            preparedStatement.setString(1, value.getId());  // 占位符的index从1开始
            preparedStatement.setLong(2, value.getTs());
            preparedStatement.setInt(3, value.getVc());
            // preparedStatement.setInt(4, value.getVc());
            // 执行SQL
            preparedStatement.execute();
            // 提交, MySQL 自动提交，此处不需要调用
            // connection.commit();
            // 关闭预处理
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }
}
