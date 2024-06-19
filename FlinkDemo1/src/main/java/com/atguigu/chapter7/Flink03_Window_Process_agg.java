package com.atguigu.chapter7;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import com.beust.ah.A;
import org.apache.flink.api.common.functions.AggregateFunction;
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
 *          输入和输出的类型必须一致
 *     aggregate 解决reduce处理不了的问题
 *          输入和输出的类型可以不一致
 *          中间有累加器做转换
 * 全量处理
 *      process 如果aggregate不满足，可以使用process
 *          Iterable<IN> process(Iterable<IN> elements 存储窗口内所有的数据
 */
public class Flink03_Window_Process_agg {
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
                // 统计每个传感器的平均水位
                .aggregate(new AggregateFunction<WaterSensor, Avg, Double>() {
                               // 初始化一个累加器
                               // 每个窗口执行一次
                               @Override
                               public Avg createAccumulator() {
                                   System.out.println("createAccumulator");
                                   return new Avg();
                               }

                               // 累加方法
                               // 每来一条数据，执行一次
                               @Override
                               public Avg add(WaterSensor value, Avg accumulator) {
                                   System.out.println("add");
                                   accumulator.sum += value.getVc();
                                   accumulator.count++;
                                   return accumulator;
                               }

                               // 返回最终累加结果
                               // 每个窗口执行一次
                               @Override
                               public Double getResult(Avg accumulator) {
                                   System.out.println("getResult");
                                   return (double) (accumulator.sum / accumulator.count);
                               }

                               // 合并累加器
                               // 注意：这个方法只在Session会话窗口会调用，其他类型的窗口不会调用
                               @Override
                               public Avg merge(Avg a, Avg b) {
                                   System.out.println("merge");
                                   return null;
                               }
                           },
                        // 窗口函数，获取当前窗口信息
                        new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                ProcessWindowFunction<Double, String, String, TimeWindow>.Context ctx,
                                                Iterable<Double> elements,
                                                Collector<String> out) throws Exception {
                                System.out.println("------");
                                System.out.println("ProcessWindowFunction");
                                long start = ctx.window().getStart();
                                long end = ctx.window().getEnd();
                                System.out.println(AtguiguUtil.toDateTime(start));
                                System.out.println(AtguiguUtil.toDateTime(end));
                                Double result = elements.iterator().next();
                                out.collect(String.valueOf(result));
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

    public static class Avg{
        public Integer sum = 0;
        public Long count = 0L;
    }
}
