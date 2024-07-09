package com.atguigu.chapter07.watermark;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.eventtime.*;
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
 * @description: Flink 水印  并行度不为1的情况下，水印的传递机制，传递水印最小的
 *
 * 总结: 多并行度的条件下, 向下游传递WaterMark的时候, 总是以最小的那个WaterMark为准! 木桶原理!
 *
 * 多并行度下，数据倾斜会导致WaterMark不更新，解决方案：1.修改为单并行度（不好，降低系统处理性能），2. 解决数据倾斜问题（推荐），3. 设置最大空闲时间
 * 若某个并行度的水印在一段时间内没有更新，那么该并行度上的水印不会往下传递
 * 如果达到最大空闲时间，则水印会自动往后传递
 */
public class Flink03_watermark_pall2 {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(2);

        // Flink程序主逻辑
        env.getConfig().setAutoWatermarkInterval(2000);  // 设置默认自动添加水印的时间, ms
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
                                .withIdleness(Duration.ofSeconds(5))  // 设置最大空闲时间，解决数据倾斜导致水印不更新的问题
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
