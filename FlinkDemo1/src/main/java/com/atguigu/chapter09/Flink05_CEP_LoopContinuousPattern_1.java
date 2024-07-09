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
 * 9.4.3模式知识补充
 * 循环模式的连续性
 * 前面的连续性也可以运用在单个循环模式中. 连续性会被运用在被接受进入模式的事件之间。
 * 松散连续
 * 默认是松散连续
 */
public class Flink05_CEP_LoopContinuousPattern_1 {
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
                .times(2)  // 连续性：默认是followBy i.e.,松散连续
                //.consecutive()  // 改为严格连续
                .allowCombinations()  // 改为非确定的松散连续 允许组合
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
