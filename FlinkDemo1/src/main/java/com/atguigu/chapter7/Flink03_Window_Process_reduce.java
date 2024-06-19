package com.atguigu.chapter7;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/18 21:13
 * @contact: yuanxin9997@qq.com
 * @description: Flink 窗口处理函数
 * 窗口内的元素进行处理，用到窗口处理函数，分两大类：
 * 增量处理
 *     简单聚合
 *        sum
 *        max
 *        min
 *        maxBy
 *        minBy
 *     reduce
 *     aggregate 解决reduce处理不了的问题
 * 全量处理
 *      process
 *          Iterable<IN> process(Iterable<IN> elements 存储窗口内所有的数据
 */
public class Flink03_Window_Process_reduce {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.parseLong(data[1]), Integer.parseInt(data[2]));
                })
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))  // 定义长度为5s的滚动窗口
                //.sum("vc")
                //.reduce(new ReduceFunction<WaterSensor>() {
                //    @Override
                //    public WaterSensor reduce(WaterSensor value1, // 上一次聚合的结果
                //                              WaterSensor value2) throws Exception {  // 本次需要参与聚合的元素
                //        System.out.println("reduce");
                //        value1.setVc(value1.getVc() + value2.getVc());
                //        return value1;  // 来一个元素，reduce就立马处理，叫做增量。但是下面print执行要等到窗口结束
                //    }
                //})
                .reduce(new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, // 上一次聚合的结果
                                                      WaterSensor value2) throws Exception {  // 本次需要参与聚合的元素
                                System.out.println("reduce");
                                value1.setVc(value1.getVc() + value2.getVc());
                                return value1;  // 来一个元素，reduce就立马处理，叫做增量。但是下面print执行要等到窗口结束
                            }
                        },
                        // 前面聚合函数的输出是这个窗口处理函数的输入. 此函数可以用来获取窗口信息
                        // 在函数在串口结束时候执行
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String key,  // key
                                                ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context ctx,  // 上下文
                                                Iterable<WaterSensor> elements,  // 这个集合有且仅有1个元素，就是前面聚合的最终结果
                                                Collector<String> out) throws Exception {
                                System.out.println("------");
                                System.out.println("ProcessWindowFunction");
                                long start = ctx.window().getStart();
                                long end = ctx.window().getEnd();
                                System.out.println(AtguiguUtil.toDateTime(start));
                                System.out.println(AtguiguUtil.toDateTime(end));
                                WaterSensor result = elements.iterator().next();
                                out.collect("" + result);
                            }
                        }
                )
                .print()
                ;

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
