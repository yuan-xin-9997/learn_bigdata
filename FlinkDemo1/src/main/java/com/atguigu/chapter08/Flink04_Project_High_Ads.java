package com.atguigu.chapter08;

import com.atguigu.bean.AdsClickLog;
import com.atguigu.bean.HotItem;
import com.atguigu.bean.UserBehavior;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月8日18:45:15
 * @contact: yuanxin9997@qq.com
 * @description:8.3页面广告分析  8.3.1页面广告点击量统计   统计每个用户对每个广告的点击量 + 黑名单过滤
 * 电商网站的市场营销商业指标中，除了自身的APP推广，还会考虑到页面上的广告投放（包括自己经营的产品和其它网站的广告）。所以广告相关的统计分
 * 析，也是市场营销的重要指标。
 * 对于广告的统计，最简单也最重要的就是页面广告的点击量，网站往往需要根据广告点击量来制定定价策略和调整推广方式，而且也可以借此收集用户的
 * 偏好信息。更加具体的应用是，我们可以根据用户的地理位置进行划分，从而总结出不同省份用户对不同广告的偏好，这样更有助于广告的精准投放。
 * 在之前的需求实现中，已经统计的广告的点击次数总和，但是没有实现窗口操作，并且也未增加排名处理.
 *
 *
 * 8.3.2黑名单过滤
 * 我们进行的点击量统计，同一用户的重复点击是会叠加计算的。
 * 在实际场景中，同一用户确实可能反复点开同一个广告，这也说明了用户对广告更大的兴趣；但是如果用户在一段时间非常频繁地点击广告，这显然不是一个正常行为，有刷点击量的嫌疑。
 * 所以我们可以对一段时间内（比如一天内）的用户点击行为进行约束，如果对同一个广告点击超过一定限额（比如100次），应该把该用户加入黑名单并报警，此后其点击行为不应该再统计。
 * 两个功能:
 * 1. 告警: 使用侧输出流
 * 2. 已经进入黑名单的用户的广告点击记录不再进行统计
 */
public class Flink04_Project_High_Ads {
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
        SingleOutputStreamOperator<String> main = env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\AdClickLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new AdsClickLog(
                            Long.parseLong(data[0]),
                            Long.parseLong(data[1]),
                            data[2],
                            data[3],
                            Long.parseLong(data[4]) * 1000
                    );
                })
                .keyBy(log -> log.getUserId() + "_" + log.getAdId())
                .process(new KeyedProcessFunction<String, AdsClickLog, String>() {

                    private ValueState<String> yesterdayState;  // 记录昨天时间，用来判断是否跨天
                    private ValueState<Boolean> isAddedToBlackListState;  // 记录用户对某个广告的点击行为是否加入黑名单
                    private ReducingState<Long> clickCountState;  // 记录用户对广告的点击次数

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        clickCountState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Long>("clickCountState", Long::sum, Long.class));
                        isAddedToBlackListState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isAddedToBlackListState", Boolean.class));
                        yesterdayState = getRuntimeContext().getState(new ValueStateDescriptor<String>("yesterdayState", String.class));
                    }

                    @Override
                    public void processElement(AdsClickLog log,
                                               KeyedProcessFunction<String, AdsClickLog, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        // 判断跨天
                        String today = AtguiguUtil.toDate(log.getTimestamp());
                        if (!today.equals(yesterdayState.value())) {  // 如果今天和状态中的日期不等
                            yesterdayState.update(today);
                            // 清空其他两个状态
                            clickCountState.clear();
                            isAddedToBlackListState.clear();
                        }

                        // 没有加入黑名单的用户对广告的点击行为，才需要累加点击次数
                        if (isAddedToBlackListState.value() == null) {
                            clickCountState.add(1L);
                        }

                        // 判断是否超过阈值
                        Long count = clickCountState.get();
                        String msg = "用户：" + log.getUserId() + "对广告：" + log.getAdId() + "的点击量：" + count;
                        if (count >= 100) {
                            if (isAddedToBlackListState.value() == null) {
                                ctx.output(new OutputTag<String>("blackList"){}, msg + " 超过阈值100，加入黑名单...");
                                isAddedToBlackListState.update(true);
                            }
                        } else {
                            out.collect(msg);
                        }

                    }
                });

        main.print("正常数据");
        main.getSideOutput(new OutputTag<String>("blackList"){}).print("黑名单");

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
