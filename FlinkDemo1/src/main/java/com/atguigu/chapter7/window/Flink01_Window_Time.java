package com.atguigu.chapter7.window;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/18 21:13
 * @contact: yuanxin9997@qq.com
 * @description: Flink 窗口
 * 时间窗口包含一个开始时间戳(包括)和结束时间戳(不包括), 这两个时间戳一起限制了窗口的尺寸.
 * 在代码中, Flink使用TimeWindow这个类来表示基于时间的窗口.  这个类提供了key查询开始时间戳和结束时间戳的方法, 还提供了针对给定的窗口获取它允许的最大时间戳的方法(maxTimestamp())
 * 时间窗口又分3种:
 * 滚动窗口(Tumbling Windows)
 * 滚动窗口有固定的大小, 窗口与窗口之间不会重叠也没有缝隙.比如,如果指定一个长度为5分钟的滚动窗口, 当前窗口开始计算, 每5分钟启动一个新的窗口.
 * 滚动窗口能将数据流切分成不重叠的窗口，每一个事件只能属于一个窗口。
 *
 * 说明:
 * 1.时间间隔可以通过: Time.milliseconds(x), Time.seconds(x), Time.minutes(x),等等来指定.
 * 2.我们传递给window函数的对象叫窗口分配器.
 * 滑动窗口(Sliding Windows)
 * 与滚动窗口一样, 滑动窗口也是有固定的长度. 另外一个参数我们叫滑动步长, 用来控制滑动窗口启动的频率.
 * 所以, 如果滑动步长小于窗口长度, 滑动窗口会重叠. 这种情况下, 一个元素可能会被分配到多个窗口中
 * 例如, 滑动窗口长度10分钟, 滑动步长5分钟, 则, 每5分钟会得到一个包含最近10分钟的数据.
 *
 *
 * 会话窗口(Session Windows)
 * 会话窗口分配器会根据活动的元素进行分组. 会话窗口不会有重叠, 与滚动窗口和滑动窗口相比, 会话窗口也没有固定的开启和关闭时间.
 * 如果会话窗口有一段时间没有收到数据, 会话窗口会自动关闭, 这段没有收到数据的时间就是会话窗口的gap(间隔)
 * 我们可以配置静态的gap, 也可以通过一个gap extractor 函数来定义gap的长度. 当时间超过了这个gap, 当前的会话窗口就会关闭, 后序的元素会被分配到一个新的会话窗口
 *
 * 示例代码:
 * 1.静态gap
 * .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
 * 2.动态gap
 * .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
 *     @Override
 *     public long extract(Tuple2<String, Long> element) { // 返回 gap值, 单位毫秒
 *         return element.f0.length() * 1000;
 *     }
 * }))
 * 创建原理:
 * 因为会话窗口没有固定的开启和关闭时间, 所以会话窗口的创建和关闭与滚动,滑动窗口不同. 在Flink内部, 每到达一个新的元素都会创建一个新的会话窗口, 如果这些窗口彼此相距比较定义的gap小, 则会对他们进行合并. 为了能够合并, 会话窗口算子需要合并触发器和合并窗口函数: ReduceFunction, AggregateFunction, or ProcessWindowFunction
 *
 */
public class Flink01_Window_Time {
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
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))  // 定义长度为5s的滚动窗口
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))  // 定义长度为5s，滑动步长为2s的滑动窗口
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))  // 定义长度为10s的会话窗口，每一个Key都有自己的窗口
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    // process方法在窗口关闭的时候触发时执行，也就是5s执行一次
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context ctx,  // 上下文，里面封装了一些信息，比如窗口的开始、结束时间，定时服务器
                                        Iterable<WaterSensor> elements,  // 存储当期窗口内所有的元素
                                        Collector<String> out) throws Exception {
                        System.out.println("--------------------------------------");
                        // 把Iterable中所有元素取出存入到list中
                        List<WaterSensor> list = AtguiguUtil.toList(elements);
                        // 获取窗口相关信息
                        long start = ctx.window().getStart();
                        long end = ctx.window().getEnd();
                        System.out.println(AtguiguUtil.toDateTime(start));
                        System.out.println(AtguiguUtil.toDateTime(end));
                        // 打印key信息
                        System.out.println(key);
                        // 打印list信息
                        System.out.println(list);
                    }
                })
                .print()
                ;

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
