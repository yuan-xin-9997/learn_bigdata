package com.atguigu.chapter05_sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/11 20:29
 * @contact: yuanxin9997@qq.com
 * @description: Flink Redis Sink
 */
public class Flink02_Redis_sink_3 {
    public static void main(String [] Args) {
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
                //.map(bean -> JSON.toJSONString(bean));
        // 输出到Redis中
        FlinkJedisConfigBase redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .setMaxTotal(100)
                .setMaxIdle(10)
                .setTimeout(10 * 1000)
                .setPassword("123456")
                .build();
        result.addSink(new RedisSink(
                redisConfig,  // Redis连接配置
                new RedisMapper<WaterSensor>() {
                    // 返回命令描述符
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        // 设置写入到Reids的方法，HSET：Hash
                        // （1）Redis hash是一个键值对集合
                        //（2）Redis hash的值是由多个field和value组成的映射表
                        //（3）类似Java里面的Map<String,Object>
                        return new RedisCommandDescription(RedisCommand.HSET, "s");  // "s"是hash外层的Key
                    }

                    // 设置写入到Redis的key
                    @Override
                    public String getKeyFromData(WaterSensor data) {
                        return data.getId();  // 对hash来说，是内部的key: field
                    }

                    // 设置写入到Redis的value
                    @Override
                    public String getValueFromData(WaterSensor data) {
                        return JSON.toJSONString(data);
                    }
                }
        ));

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
