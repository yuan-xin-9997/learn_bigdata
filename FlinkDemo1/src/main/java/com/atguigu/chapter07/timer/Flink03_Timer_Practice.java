package com.atguigu.chapter07.timer;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/24 20:24
 * @contact: yuanxin9997@qq.com
 * @description: Flink 定时器练习
 * 7.7.3定时器练习
 * 需求：
 *      监控水位传感器的水位值，如果水位值在5s之内(event time)连续上升，则报警。
 *
 * 分析：
 *    使用滚动、滑动时间窗口都不好，不能完美实现需求，
 *    会话窗口不适合
 *    定时器适合
 */
public class Flink03_Timer_Practice {
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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTs())
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    int lastVc = 0;  // 上一次的水位
                    private long ts;

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 如果是第一个水位数据，则注册5s后触发的定时器
                        // 如果不是第一个水位，判断当前水位是否大于上一次的水位，如果上升，则什么都不做，如果下降或相等，删除定时器，重新注册一个新的5s后报警的定时器
                        if (lastVc == 0) {
                            ts = value.getTs() + 5000;
                            System.out.println("注册定时器：" + ts);
                            ctx.timerService().registerEventTimeTimer(ts);
                        }else if (value.getVc() <= lastVc) {
                            // 水位下降或相等，取消定时器
                            System.out.println("水位下降或不变，删除定时器：" + ts);
                            ctx.timerService().deleteEventTimeTimer(ts);
                            ts = value.getTs() + 5000;
                            System.out.println("重新注册定时器：" + ts);
                        }else{
                            // 水位上升
                            System.out.println("水位上升，什么都不做");
                        }
                        lastVc = value.getVc();
                    }

                    // 定时器触发时执行
                    @Override
                    public void onTimer(long timestamp,  // 定时器触发时间
                                        KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx,  // 上下文
                                        Collector<String> out) throws Exception {  // 输出流
                        out.collect(ctx.getCurrentKey() + "连续5s水位上升，发出预警....");
                        // 重新注册5s后触发的定时器
                        lastVc = 0;// 投机取巧的方法
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
