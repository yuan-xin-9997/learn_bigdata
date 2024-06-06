package com.atguigu.chapter05_sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.ArrayList;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/06 21:49
 * @contact: yuanxin9997@qq.com
 * @description: Flink Kafka sink
 */
public class Flink01_kafka_sink {
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

        // 2. 输出到Kafka
        waterSensorDS.keyBy(WaterSensor::getId)
                .sum("vc")
                .map(bean -> JSON.toJSONString(bean))
                .addSink(new FlinkKafkaProducer<>())

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
