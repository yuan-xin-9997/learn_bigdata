package com.atguigu.chapter07.watermark;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/20 19:23
 * @contact: yuanxin9997@qq.com
 * @description: Flink 水印
 *
 * 7.3.4Flink中如何产生水印
 * 在 Flink 中， 水印由应用程序开发人员生成， 这通常需要对相应的领域有 一定的了解。完美的水印永远不会错：时间戳小于水印标记时间的事件不会
 * 再出现。在特殊情况下（例如非乱序事件流），最近一次事件的时间戳就可能是完美的水印。
 * 启发式水印则相反，它只估计时间，因此有可能出错， 即迟到的事件 （其时间戳小于水印标记时间）晚于水印出现。针对启发式水印， Flink 提供了处理迟到元素的机制。
 * 设定水印通常需要用到领域知识。举例来说，如果知道事件的迟到时间不会超过 5 秒， 就可以将水印标记时间设为收到的最大时间戳减去 5 秒。 另 一
 * 种做法是，采用一个 Flink 作业监控事件流，学习事件的迟到规律，并以此构建水印生成模型。
 *
 * 7.3.5EventTime和WaterMark的使用
 *
 * Flink内置了两个WaterMark生成器:
 * 1.Monotonously Increasing Timestamps(时间戳单调增长:其实就是允许的延迟为0)
 * WatermarkStrategy.forMonotonousTimestamps();
 *
 * 2.Fixed Amount of Lateness(允许固定时间的延迟)
 * WatermarkStrategy.forBoundedOutOfOrderness
 */
public class Flink01_watermark_1 {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // 设置时间语义
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Flink程序主逻辑
        env
                .socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                })
                .assignTimestampsAndWatermarks(  // 分配时间戳和水印
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context ctx,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        List<WaterSensor> list = AtguiguUtil.toList(elements);
                        //System.out.println("------");
                        //System.out.println("ProcessWindowFunction");
                        String start = AtguiguUtil.toDateTime(ctx.window().getStart());
                        String end = AtguiguUtil.toDateTime(ctx.window().getEnd());
                        //System.out.println(AtguiguUtil.toDateTime(start));
                        //System.out.println(AtguiguUtil.toDateTime(end));
                        //System.out.println(list);
                        out.collect("key: " + key + ", start: " + start + ", end: " + end + ", list: " + list);
                    }
                })
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
