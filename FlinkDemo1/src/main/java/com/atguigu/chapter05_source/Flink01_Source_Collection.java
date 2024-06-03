package com.atguigu.chapter05_source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2020/12/8 22:44
 * @Description: Flink 从 Java集合中读取数据
 * 一般情况下，可以将数据临时存储到内存中，形成特殊的数据结构后，作为数据源使用。这里的数据结构采用集合类型是比较普遍的。
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        List<WaterSensor> waterSensors = Arrays.asList(
          new WaterSensor("ws_001", 1577844001L, 45),
          new WaterSensor("ws_002", 1577844015L, 43),
          new WaterSensor("ws_003", 1577844020L, 42));

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
          .fromCollection(waterSensors)
         .print();
        env.execute();
    }
}