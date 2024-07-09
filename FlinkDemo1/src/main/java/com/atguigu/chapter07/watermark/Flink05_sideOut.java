package com.atguigu.chapter07.watermark;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/20 19:23
 * @contact: yuanxin9997@qq.com
 * @description: Flink 水印  侧输出流
 * 7.5侧输出流(sideOutput)
 * 7.5.1接收窗口关闭之后的迟到数据
 * 允许迟到数据, 窗口也会真正的关闭, 如果还有迟到的数据怎么办?  Flink提供了一种叫做侧输出流的来处理关窗之后到达的数据.
 *
 * Flink 如何保证数据不丢失：
 * 1. 水印机制 + 事件事件
 * 2. 允许迟到机制
 * 3. 侧输出流
 */
public class Flink05_sideOut {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        env.getConfig().setAutoWatermarkInterval(2000);  // 设置默认自动添加水印的时间, ms
        SingleOutputStreamOperator<String> main = env
                .socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                })
                .assignTimestampsAndWatermarks(  // 分配时间戳和水印
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))  // 最大容忍的延迟时间
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                        //.withIdleness(Duration.ofSeconds(5))  // 设置最大空闲时间，解决数据倾斜导致水印不更新的问题
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))  // 允许迟到时间：2s，当到了窗口的关闭时间，窗口先不关闭，但会对窗口元素先做计算，过2s后窗口真正的关闭。再迟到的数据不会参与计算
                .sideOutputLateData(new OutputTag<WaterSensor>("late"){})  // 如果不加泛型以及{}，会报错ould not determine TypeInformation for the OutputTag type. The most common reason is forgetting to make the OutputTag an anonymous inner class. It is also not possible to use generic type variables with OutputTags, such as 'Tuple2<A, B>'.
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    // 在允许迟到期间，每来1个属于这个窗口的元素，这个方法就会执行
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
                });

        main.print("main");
        main.getSideOutput(new OutputTag<WaterSensor>("late"){}).print("late");

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
