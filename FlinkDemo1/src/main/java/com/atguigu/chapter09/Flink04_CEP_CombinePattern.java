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

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月9日18:44:56
 * @contact: yuanxin9997@qq.com
 * @description: 第9章Flink CEP编程
 *
 * 9.4.2组合模式(模式序列)
 * 把多个单个模式组合在一起就是组合模式.  组合模式由一个初始化模式(.begin(...))开头
 * 严格连续(严格紧邻)
 * 期望所有匹配的事件严格的一个接一个出现，中间没有任何不匹配的事件
 *
 * 注意:
 * notNext  如果不想后面直接连着一个特定事件
 *
 *
 *
 * 松散连续
 * 忽略匹配的事件之间的不匹配的事件。
 *
 * 注意:
 * 	notFollowBy 如果不想一个特定事件发生在两个事件之间的任何地方。(notFollowBy不能位于模式的最后)
 */
public class Flink04_CEP_CombinePattern {
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
                //.next("s2")  // 严格连续(严格紧邻)  按照时间水印 定义的严格连续 而不是输入数据的先后
                //.notNext("s2")  // 严格连续(严格紧邻) 找sensor_1后面有数据，但是不是sensor_2
                //.followedBy("s2")  // 松散连续(中间可以跟一个其他者）  s1和s2之间可以跟一个中间者
                .notFollowedBy("s2")  // 松散连续(中间可以跟一个其他者） s1后面不可以有s2 NotFollowedBy is not supported as a last part of a Pattern
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                })
                .followedBy("s3")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_3".equals(value.getId());
                    }
                })
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
