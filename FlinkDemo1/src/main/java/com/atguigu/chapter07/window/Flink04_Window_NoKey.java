package com.atguigu.chapter07.window;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/18 21:13
 * @contact: yuanxin9997@qq.com
 * @description: Flink 窗口处理函数
 * 窗口内的元素进行处理，用到窗口处理函数  Window Function ，分两大类：
 * 增量处理
 *     简单聚合
 *        sum
 *        max
 *        min
 *        maxBy
 *        minBy
 *     reduce ReduceFunction
 *          输入和输出的类型必须一致
 *     aggregate 解决reduce处理不了的问题  AggregateFunction
 *          输入和输出的类型可以不一致
 *          中间有累加器做转换
 * 全量处理   对内存占用大，相比于增量处理
 *      process 如果aggregate不满足，可以使用process    ProcessWindowFunction
 *          Iterable<IN> process(Iterable<IN> elements 存储窗口内所有的数据
 *
 *
 * 窗口类型
 * windowAll(): 所有的元素都会进入到同一个窗口中
 *    比如：0-5时间，所有元素进入到同一个窗口
 *
 * 7.2Keyed vs Non-Keyed Windows
 * 其实, 在用window前首先需要确认应该是在keyBy后的流上用, 还是在没有keyBy的流上使用.
 * 在keyed streams上使用窗口, 窗口计算被并行的运用在多个task上, 可以认为每个task都有自己单独窗口. 正如前面的代码所示.
 * 在非non-keyed stream上使用窗口, 流的并行度只能是1, 所有的窗口逻辑只能在一个单独的task上执行.
 * .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
 * 需要注意的是: 非key分区的流上使用window, 如果把并行度强行设置为>1, 则会抛出异常
 */
public class Flink04_Window_NoKey {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(2);

        // Flink程序主逻辑
        env.socketTextStream("hadoop102", 9999)  // 并行度是1
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.parseLong(data[1]), Integer.parseInt(data[2]));
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("vc")  // windowAll的窗口处理函数，并行度是1（也不可以被设置，设置会报错），否则不在同一个并行度的sum没有意义
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
