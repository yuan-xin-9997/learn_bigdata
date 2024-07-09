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
 * 条件
 * 对每个模式你可以指定一个条件来决定一个进来的事件是否被接受进入这个模式，例如前面用到的where就是一种条件
 * 迭代条件
 * 这是最普遍的条件类型。使用它可以指定一个基于前面已经被接受的事件的属性或者它们的一个子集的统计数据来决定是否接受时间序列的条件。
 *
 * 简单条件
 * 这种类型的条件扩展了前面提到的IterativeCondition类，它决定是否接受一个事件只取决于事件自身的属性。
 *
 * 组合条件
 * 把多个条件结合起来使用. 这适用于任何条件，你可以通过依次调用where()来组合条件。 最终的结果是每个单一条件的结果的逻辑AND。
 * 如果想使用OR来组合条件，你可以像下面这样使用or()方法。
 *
 * 停止条件
 * 如果使用循环模式(oneOrMore, timesOrMore), 可以指定一个停止条件, 否则有可能会内存吃不消.
 * 意思是满足了给定的条件的事件出现后，就不会再有事件被接受进入模式了。
 */
public class Flink03_CEP_ConditionPattern {
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
//                .where(new SimpleCondition<WaterSensor>() {
//                    @Override
//                    public boolean filter(WaterSensor value) throws Exception {
//                        return value.getVc() > 20;
//                    }
//                })
                .or(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc() > 20;
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
