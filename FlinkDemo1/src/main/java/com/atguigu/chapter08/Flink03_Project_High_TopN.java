package com.atguigu.chapter08;

import com.atguigu.bean.HotItem;
import com.atguigu.bean.UserBehavior;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/04 20:43
 * @contact: yuanxin9997@qq.com
 * @description: 8.2电商数据分析  8.2.1实时热门商品统计 todo 此Demo没有非常理解
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
 */
public class Flink03_Project_High_TopN {
    public static void main(String[] Args) {
        System.out.println("Flink 流处理高阶编程实战");
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(3);

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
                .keyBy(UserBehavior::getItemId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(UserBehavior value, Long accumulator) {
                                   return accumulator + 1;
                               }

                               @Override
                               public Long getResult(Long accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public Long merge(Long a, Long b) {
                                   return null;
                               }
                           },
//                        new WindowFunction<Long, HotItem, Long, TimeWindow>() {
//                            @Override
//                            public void apply(Long itemId,
//                                              TimeWindow window,
//                                              Iterable<Long> input,
//                                              Collector<HotItem> out) throws Exception {
//
//                            }
//                        }
                        new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                            @Override
                            public void process(Long itemId,
                                                ProcessWindowFunction<Long, HotItem, Long, TimeWindow>.Context context,
                                                Iterable<Long> elements,
                                                Collector<HotItem> out) throws Exception {
                                HotItem hotItem = new HotItem(itemId, context.window().getEnd(), elements.iterator().next());
                                out.collect(hotItem);
                            }
                        }
                )
                .keyBy(HotItem::getWindowEnd)
                .process(new KeyedProcessFunction<Long, HotItem, String>() {

                    private ListState<HotItem> hotItemState;

                    // 从状态中取出所有值，排序TopN
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, HotItem, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        List<HotItem> hotItems = AtguiguUtil.toList(hotItemState.get());
                        hotItems.sort(new Comparator<HotItem>() {
                            @Override
                            public int compare(HotItem o1, HotItem o2) {
                                return o2.getCount().compareTo(o1.getCount());
                            }
                        });
                        String msg = "\n----------------------\n";
                        for (int i = 0; i < Math.min(3, hotItems.size()); i++) {
                            HotItem hotItem = hotItems.get(i);
                            msg += hotItem + "\n";
                        }
                        out.collect(msg);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hotItemState = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("hotItemState", HotItem.class));
                    }

                    @Override
                    public void processElement(HotItem value,
                                               KeyedProcessFunction<Long, HotItem, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        // 当第一个元素来的时候注册定时器
                        // list状态中没有值
                        if (!hotItemState.get().iterator().hasNext()) {
                            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
                        }
                        // 每个元素都应该存入到状态中
                        hotItemState.add(value);
                    }
                })
                .print();

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
