package com.atguigu.chapter09;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月9日20:13:28
 * @contact: yuanxin9997@qq.com
 * @description: 第9章Flink CEP编程
 *
 * 9.4.5超时数据
 * 当一个模式上通过within加上窗口长度后，部分匹配的事件序列就可能因为超过窗口长度而被丢弃。
 * Pattern<WaterSensor, WaterSensor> pattern = Pattern
 *     .<WaterSensor>begin("start")
 *     .where(new SimpleCondition<WaterSensor>() {
 *         @Override
 *         public boolean filter(WaterSensor value) throws Exception {
 *             return "sensor_1".equals(value.getId());
 *         }
 *     })
 *     .next("end")
 *     .where(new SimpleCondition<WaterSensor>() {
 *         @Override
 *         public boolean filter(WaterSensor value) throws Exception {
 *             return "sensor_2".equals(value.getId());
 *         }
 *     })
 *     .within(Time.seconds(2));
 *
 */
public class Flink08_CEP_Within {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        // 1. 获取数据流
        SingleOutputStreamOperator<WaterSensor> stream = env
                .readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\sensor.txt")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.valueOf(fields[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((waterSensor, recordTimestamp) -> waterSensor.getTs())
                );

        // 2. 定义CEP规则（模式）
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .next("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                })
                .within(Time.seconds(2))  // 当一个模式上通过within加上窗口长度后，部分匹配的事件序列就可能因为超过窗口长度而被丢弃
                ;

        // 3. 把规则作用到流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(stream, pattern);

        // 4. 从模式流中选择出匹配的数据
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            // 参数是一个Map集合，key是模式名，value是模式匹配成功一次匹配上的所有数据（List<WaterSensor>>）
            // 每匹配成功一次，方法就执行一次
            // 会把返回值放入到一个新的流中
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        });

        result.print();

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
