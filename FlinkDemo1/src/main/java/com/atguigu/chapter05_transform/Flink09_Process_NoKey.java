package com.atguigu.chapter05_transform;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/06 20:48
 * @contact: yuanxin9997@qq.com
 * @description: Flink process 算子
 */
public class Flink09_Process_NoKey {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        // 不使用keyBy
        // 计算所有传感器的水位和
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 160924000002L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 160924000004L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 160924000006L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 160924000003L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 160924000005L, 30));
        DataStreamSource<WaterSensor> waterSensorDS = env.fromCollection(waterSensors);
        waterSensorDS.process(new ProcessFunction<WaterSensor, Integer>() {
            // 并行度是2的情况下，有2个sum，会导致计算所有传感器水位和计算错误
            // 只有并行度为1的情况下，才能计算正确
            int sum = 0;
            @Override
            public void processElement(WaterSensor value,  // 当前要处理的元素
                                       ProcessFunction<WaterSensor, Integer>.Context ctx,  // 上下文对象
                                       Collector<Integer> out) throws Exception {  // 输出流
                sum += value.getVc();
                out.collect(sum);
            }
        })
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
