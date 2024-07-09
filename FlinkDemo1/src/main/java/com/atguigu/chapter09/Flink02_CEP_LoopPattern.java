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
 * 9.4模式API
 * 模式API可以让你定义想从输入流中抽取的复杂模式序列。
 * 几个概念:
 * 模式:
 * 比如找拥有相同属性事件序列的模式(前面案例中的拥有相同的id), 我们一般把简单模式称之为模式
 * 注意:
 * 1.每个模式必须有一个独一无二的名字，你可以在后面使用它来识别匹配到的事件。(比如前面的start模式)
 * 2.模式的名字不能包含字符":"
 * 模式序列
 * 每个复杂的模式序列包括多个简单的模式，也叫模式序列. 你可以把模式序列看作是这样的模式构成的图，这些模式基于用户指定的条件从一个转换到另外一个。
 * 匹配
 * 输入事件的一个序列，这些事件通过一系列有效的模式转换，能够访问到复杂模式图中的所有模式。
 * 9.4.1单个模式
 * 单个模式可以是单例模式或者循环模式.
 * 单例模式
 * 单例模式只接受一个事件. 默认情况模式都是单例模式.
 * 前面的例子就是一个单例模式
 * 循环模式
 * 循环模式可以接受多个事件.
 * 单例模式配合上量词就是循环模式.(非常类似我们熟悉的正则表达式)
 *
 * // 1. 定义模式
 * Pattern<WaterSensor, WaterSensor> pattern = Pattern
 *     .<WaterSensor>begin("start")
 *     .where(new SimpleCondition<WaterSensor>() {
 *         @Override
 *         public boolean filter(WaterSensor value) throws Exception {
 *             return "sensor_1".equals(value.getId());
 *         }
 *     });
 *
 * // 1.1 使用量词 出现两次
 * Pattern<WaterSensor, WaterSensor> patternWithQuantifier = pattern.times(2);
 * 范围内的次数
 * // 1.1 使用量词 [2,4]   2次,3次或4次
 * Pattern<WaterSensor, WaterSensor> patternWithQuantifier = pattern.times(2, 4);
 *
 * 一次或多次
 * Pattern<WaterSensor, WaterSensor> patternWithQuantifier = pattern.oneOrMore();
 * 多次及多次以上
 * // 2次或2次一样
 * Pattern<WaterSensor, WaterSensor> patternWithQuantifier = pattern.timesOrMore(2);
 *
 *
 */
public class Flink02_CEP_LoopPattern {
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
                })
                //.times(2)  // 匹配正好2次   a{2}
                //.times(2, 4)  // 匹配2~4次   a{2,4}
                //.timesOrMore(1)  // 匹配1次或多次  a{1,}
                .oneOrMore()  //  匹配1次或多次  a{1,}
                .until(new SimpleCondition<WaterSensor>() {  // more的后面务必要加util终止条件，否则容易内存溢出
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
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
