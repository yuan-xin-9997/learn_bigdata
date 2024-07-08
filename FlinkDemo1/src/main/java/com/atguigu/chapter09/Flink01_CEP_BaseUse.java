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
 * @createTime: 2024/07/08 21:35
 * @contact: yuanxin9997@qq.com
 * @description: 第9章Flink CEP编程
 *
 * 第9章Flink CEP编程
 * 9.1什么是FlinkCEP
 * FlinkCEP(Complex event processing for Flink) 是在Flink实现的复杂事件处理库. 它可以让你在无界流中检测出特定的数据，有机会掌握数据中重要的那部分。
 * 是一种基于动态环境中事件流的分析技术，事件在这里通常是有意义的状态变化，通过分析事件间的关系，利用过滤、关联、聚合等技术，根据事件间的时序关系和聚合关系制定检测规则，持续地从事件流中查询出符合要求的事件序列，最终分析得到更复杂的复合事件。
 * 1.目标：从有序的简单事件流中发现一些高阶特征
 * 2.输入：一个或多个由简单事件构成的事件流
 * 3.处理：识别简单事件之间的内在联系，多个符合一定规则的简单事件构成复杂事件
 * 4.输出：满足规则的复杂事件
 *
 * 9.2Flink CEP应用场景
 * 风险控制（类似监查系统）
 * 对用户异常行为模式进行实时检测，当一个用户发生了不该发生的行为，判定这个用户是不是有违规操作的嫌疑。
 * 策略营销
 * 用预先定义好的规则对用户的行为轨迹进行实时跟踪，对行为轨迹匹配预定义规则的用户实时发送相应策略的推广。
 * 运维监控
 * 灵活配置多指标、多依赖来实现更复杂的监控模式。
 *
 * 9.3CEP开发基本步骤
 * 9.3.1导入CEP相关依赖
 * <dependency>
 *     <groupId>org.apache.flink</groupId>
 *     <artifactId>flink-cep_${scala.binary.version}</artifactId>
 *     <version>${flink.version}</version>
 * </dependency>
 * 9.3.2基本使用
 *
 * CEP使用步骤：
 * 1. 先有1个流
 * 2. 定义规则（模式）
 * 3. 把规则作用到流，得到一个模式流
 * 4. 从模式流中选择出匹配的数据
 */
public class Flink01_CEP_BaseUse {
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

        // 2. 定义CEP规则
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                });

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
