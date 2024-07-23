package com.atguigu.chapter13;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/22 20:47
 * @contact: yuanxin9997@qq.com
 * @description: Flink 双流Join - Interval Join
 *
 * 第13章一些补充知识
 * 13.1双流join
 * 在Flink中, 支持两种方式的流的Join: Window Join和Interval Join
 * 13.1.1Window Join
 * 窗口join会join具有相同的key并且处于同一个窗口中的两个流的元素.
 * 注意:
 * 1.所有的窗口join都是 inner join, 意味着a流中的元素如果在b流中没有对应的, 则a流中这个元素就不会处理(就是忽略掉了)
 * 2.join成功后的元素的会以所在窗口的最大时间作为其时间戳.  例如窗口[5,10), 则元素会以9作为自己的时间戳
 * 滚动窗口Join
 *
 * 13.1.2Interval Join
 * 间隔流join(Interval Join), 是指使用一个流的数据按照key去join另外一条流的指定范围的数据.
 * 如下图: 橙色的流去join绿色的流.范围是由橙色流的event-time + lower bound和event-time + upper bound来决定的.
 * orangeElem.ts + lowerBound <= greenElem.ts <= orangeElem.ts + upperBound
 *
 * 注意:
 * 1)Interval Join只支持event-time
 * 2)必须是keyBy之后的流才可以interval join
 *
 */
public class Flink02_Join_IntervalJoin {
    public static void main(String[] Args) {
        // 设置环境变量
        setProperty("HADOOP_USER_NAME", "atguigu");

        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        // 流s1
        KeyedStream<WaterSensor, String> s1 = env
            .socketTextStream("hadoop102", 8888)  // 在socket终端只输入毫秒级别的时间戳
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forMonotonousTimestamps()
                    .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                        @Override
                        public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                            return element.getTs() * 1000;
                        }
                    })
            )
                .keyBy(WaterSensor::getId)
                ;

        // 流s2
        KeyedStream<WaterSensor, String> s2 = env
            .socketTextStream("hadoop102", 9999)  // 在socket终端只输入毫秒级别的时间戳
            .map(value -> {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forMonotonousTimestamps()
                    .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                        @Override
                        public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                            return element.getTs() * 1000;
                        }
                    })
            )
                .keyBy(WaterSensor::getId);

        // 双流join
        s1
                .intervalJoin(s2)
                .between(Time.seconds(-5), Time.seconds(2))  // 指定join的区间
                .lowerBoundExclusive()  // 默认包含边界，此处把边界排除
                .process(new ProcessJoinFunction<WaterSensor, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor left, WaterSensor right, ProcessJoinFunction<WaterSensor, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "<>" + right);
                    }
                })
                .print()
                ;

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
