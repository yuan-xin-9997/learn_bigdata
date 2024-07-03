package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.jws.soap.SOAPBinding;
import java.time.Duration;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/03 21:01
 * @contact: yuanxin9997@qq.com
 * @description: Flink 第8章Flink流处理高阶编程实战 8.1基于埋点日志数据的网络流量统计 8.1.1指定时间范围内网站总浏览量（PV）的统计
 */
public class Flink01_Project_High_PV {
    public static void main(String[] Args) {
        System.out.println("Flink 流处理高阶编程实战");
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\UserBehavior.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new UserBehavior(
                            Long.parseLong(data[0]),
                            Long.parseLong(data[1]),
                            Integer.parseInt(data[2]),
                            data[3],
                            Long.parseLong(data[4]) * 1000
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                )
                .filter(event -> "pv".equals(event.getBehavior()))
                .windowAll(SlidingEventTimeWindows.of(Time.hours(2), Time.minutes(10)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(UserBehavior userBehavior, Long acc) {
                                   return acc + 1;
                               }

                               @Override
                               public Long getResult(Long acc) {
                                   return acc;
                               }

                               @Override
                               public Long merge(Long aLong, Long acc1) {
                                   return null;
                               }
                           },
                        new ProcessAllWindowFunction<Long, String, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<Long, String, TimeWindow>.Context ctx,
                                                Iterable<Long> elements,
                                                Collector<String> out) throws Exception {
                                Long res = elements.iterator().next();
                                String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                                String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());
                                out.collect("窗口：" + stt + "~" + edt + "，pv数：" + res);
                            }
                        }
                )
                .print();

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
