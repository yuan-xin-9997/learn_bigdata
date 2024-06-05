package com.atguigu.chapter05_transform;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/05 21:47
 * @contact: yuanxin9997@qq.com
 * @description: Flink reduce 算子
 */
public class Flink06_reduce {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // Flink程序主逻辑
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 160924000002L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 160924000004L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 160924000006L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 160924000003L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 160924000005L, 30));
        DataStreamSource<WaterSensor> waterSensorDS = env.fromCollection(waterSensors);
        waterSensorDS.keyBy(WaterSensor::getId)  // todo 方法引用 新特性   当返回结果是一个类的方法 那可以直接用类名::方法名的形式进行调用
                .reduce(new ReduceFunction<WaterSensor>() {
                    // reduce 进入的数据和出来的数据 类型一致
                    // ws1 是历史聚合的结果
                    // ws2 是新来的一条数据
                    @Override
                    public WaterSensor reduce(WaterSensor ws1, WaterSensor ws2) throws Exception {
                        System.out.println("=======");  // 第1条数据不会进入reduce
                        // 进入reduce之前已经已经做了keyBy，只有id相同的才能进入reduce
                        return new WaterSensor(ws1.getId(), ws2.getTs(), ws1.getVc() + ws2.getVc());
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
