package com.atguigu.chapter05_transform;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/06 20:48
 * @contact: yuanxin9997@qq.com
 * @description: Flink 算子的Rich版本
 */
public class Flink09_RichOperators {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(2);

        // Flink程序主逻辑
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 160924000002L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 160924000004L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 160924000006L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 160924000003L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 160924000005L, 30));
        DataStreamSource<WaterSensor> waterSensorDS = env.fromCollection(waterSensors);
        // 需求：每来1个元素，去查询数据库，根据查询结果对数据决定如何处理
        waterSensorDS.
                map(new RichMapFunction<WaterSensor, String>() {
                    // 程序启动的时候执行一次，每个并行度执行一次，所有的初始化已经完成，环境已经可用
                    // 一般用于建立数据库连接，创建资源，一次性工作
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("open");
                    }

                    // 程序停止的时候执行一次，每个并行度执行一次
                    // 一般用于close数据库连接，close资源，一次性工作
                    @Override
                    public void close() throws Exception {
                        System.out.println("close");
                    }

                    @Override
                    public String map(WaterSensor value) throws Exception {
                        // 建立到数据的连接

                        // 查询

                        // 获取结果

                        // 对象数据进行处理

                        // 关闭到数据库的连接

                        return value.toString();
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
