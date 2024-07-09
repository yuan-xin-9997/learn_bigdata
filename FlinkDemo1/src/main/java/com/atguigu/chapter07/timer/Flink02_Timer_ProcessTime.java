package com.atguigu.chapter07.timer;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/24 20:24
 * @contact: yuanxin9997@qq.com
 * @description: Flink 定时器  基于处理时间的定时器
 *
 * 基于处理时间或者事件时间处理过一个元素之后, 注册一个定时器, 然后指定的时间执行.
 * 	Context和OnTimerContext所持有的TimerService对象拥有以下方法:
 * currentProcessingTime(): Long 返回当前处理时间
 * currentWatermark(): Long 返回当前watermark的时间戳
 * registerProcessingTimeTimer(timestamp: Long): Unit 会注册当前key的processing time的定时器。当processing time到达定时时间时，触发timer。
 * registerEventTimeTimer(timestamp: Long): Unit 会注册当前key的event time 定时器。当水位线大于等于定时器注册的时间时，触发定时器执行回调函数。
 * deleteProcessingTimeTimer(timestamp: Long): Unit 删除之前注册处理时间定时器。如果没有这个时间戳的定时器，则不执行。
 * deleteEventTimeTimer(timestamp: Long): Unit 删除之前注册的事件时间定时器，如果没有此时间戳的定时器，则不执行。
 */
public class Flink02_Timer_ProcessTime {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        env
                .socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                })
                // 当水位超过10，5s发出预警
                // 定时器是和Key绑定在一起，必须先进行keyBy
                // 如果只有1个并行度，不同key下的定时器只会影响自己key
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private long ts;

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > 10) {
                            // 注册定时器，基于处理时间
                            ts = System.currentTimeMillis() + 5000;
                            System.out.println("注册定时器：" + ts);
                            ctx.timerService().registerProcessingTimeTimer(ts);
                        } else {
                            // 删除定时器
                            System.out.println("删除定时器：" + ts);
                            ctx.timerService().deleteProcessingTimeTimer(ts);
                        }

                        // out.collect(value.toString());
                    }

                    // 定时器触发时执行
                    @Override
                    public void onTimer(long timestamp,  // 定时器触发时间
                                        KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx,  // 上下文
                                        Collector<String> out) throws Exception {  // 输出流
                        out.collect(ctx.getCurrentKey() + "水位超过10，发出预警....");
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
