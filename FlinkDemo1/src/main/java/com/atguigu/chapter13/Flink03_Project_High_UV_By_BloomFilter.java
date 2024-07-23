package com.atguigu.chapter13;

import com.atguigu.bean.UserBehavior;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnel;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月23日20:14:04
 * @contact: yuanxin9997@qq.com
 * @description: 8.1.2指定时间范围内网站独立访客数（UV）的统计 - 使用布隆过滤器实现此需求
 *
 * 8.2电商数据分析
 * 电商平台中的用户行为频繁且较复杂，系统上线运行一段时间后，可以收集到大量的用户行为数据，进而利用大数据技术进行深入挖掘和分析，得到感兴趣的商业指标并增强对风险的控制。
 * 电商用户行为数据多样，整体可以分为用户行为习惯数据和业务行为数据两大类。
 * 用户的行为习惯数据包括了用户的登录方式、上线的时间点及时长、点击和浏览页面、页面停留时间以及页面跳转等等，我们可以从中进行流量统计和热门商品的统计，也可以深入挖掘用户的特征；这些数据往往可以从web服务器日志中直接读取到。
 * 而业务行为数据就是用户在电商平台中针对每个业务（通常是某个具体商品）所作的操作，我们一般会在业务系统中相应的位置埋点，然后收集日志进行分析。
 * 8.2.1实时热门商品统计
 * 需求分析
 * 每隔5分钟输出最近1小时内点击量最多的前N个商品
 * 最近一小时: 窗口长度
 * 每隔5分钟: 窗口滑动步长
 * 时间: 使用event-time
 *
 */
public class Flink03_Project_High_UV_By_BloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建WatermarkStrategy
        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy
            .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                @Override
                public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                    return element.getTimestamp() * 1000L;
                }
            });

        env
            .setParallelism(1)
            .readTextFile("FlinkDemo1/input/UserBehavior.csv")
            .map(line -> { // 对数据切割, 然后封装到POJO中
                String[] split = line.split(",");
                return new UserBehavior(Long.valueOf(split[0]),
                        Long.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        split[3],
                        Long.valueOf(split[4]));
            })
            .assignTimestampsAndWatermarks(wms)
            .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
            .windowAll(SlidingEventTimeWindows.of(Time.hours(2), Time.hours(2)))
            .process(new ProcessAllWindowFunction<UserBehavior, String, TimeWindow>() {
                @Override
                public void process(ProcessAllWindowFunction<UserBehavior, String, TimeWindow>.Context ctx,
                                    Iterable<UserBehavior> elements,
                                    Collector<String> out) throws Exception {
                    // 使用Google guagua创建布隆过滤器
                    // 第一个参数 布隆过滤存的数据类型，第二个参数 预计要存储的数据元素，第三个参数 希望假阳性的概率不高于此值
                    BloomFilter<Long> bloomFilter = BloomFilter.create(Funnels.longFunnel(), 100 * 10000, 0.01);
                    int count = 0;  // 记录 独立访客的数量
                    for (UserBehavior element : elements) {
                        if (bloomFilter.put(element.getUserId())) {  // 判断是否成功将数据添加到布隆过滤器中
                            count ++;
                        }
                    }
//                    bloomFilter.mightContain()
                    String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                    String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());
                    out.collect(stt + " - " + edt + " - " + count);
                }
            })
            .print();
        env.execute();
    }
}
