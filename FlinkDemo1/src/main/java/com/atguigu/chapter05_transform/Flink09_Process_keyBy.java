package com.atguigu.chapter05_transform;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/06 20:48
 * @contact: yuanxin9997@qq.com
 * @description: Flink process 算子
 */
public class Flink09_Process_keyBy {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        // keyBy之后用process
        // 计算每个传感器的水位和
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 160924000002L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 160924000004L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 160924000006L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 160924000003L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 160924000005L, 30));
        DataStreamSource<WaterSensor> waterSensorDS = env.fromCollection(waterSensors);
        waterSensorDS.
                keyBy(WaterSensor::getId ).
                process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 状态
                    // 同一个分区下的所有传感器都是共用的
                    // 并行度设置为2，计算结果正确
                    // 并行度设置为1, 每个传感器使用同一个sum，导致最后每个传感器加总到一起
                    // int sum = 0;

                    // 状态
                    // 下面这个Map数据结构，可以不管并行度是1还是2，实现的很丑陋
                    Map<String, Integer> map = new HashMap<>();
                    @Override
                    public void processElement(WaterSensor value,  // 当前要处理的元素
                                               KeyedProcessFunction<String, WaterSensor, String>.Context ctx, // 上下文
                                               Collector<String> out) throws Exception {  // 输出流
                        Integer sum = map.getOrDefault(ctx.getCurrentKey(), 0);
                        sum += value.getVc();
                        map.put(ctx.getCurrentKey(), sum);
                        out.collect(ctx.getCurrentKey() + "的水位是：" + value.getVc() + ", 总和是：" + sum);
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
