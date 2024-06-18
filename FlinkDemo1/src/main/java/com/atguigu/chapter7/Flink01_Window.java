package com.atguigu.chapter7;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/18 21:13
 * @contact: yuanxin9997@qq.com
 * @description: Flink 窗口
 */
public class Flink01_Window {
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
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))  // 定义长度为5s的滚动窗口
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
