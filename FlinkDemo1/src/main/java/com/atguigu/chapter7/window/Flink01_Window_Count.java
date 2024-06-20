package com.atguigu.chapter7.window;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/18 21:13
 * @contact: yuanxin9997@qq.com
 * @description: Flink 窗口
 *
 * 7.1.2.3基于元素个数的窗口
 * 按照指定的数据条数生成一个Window，与时间无关
 * 分2类:
 * 滚动窗口
 * 默认的CountWindow是一个滚动窗口，只需要指定窗口大小即可，当元素数量达到窗口大小时，就会触发窗口的执行。
 * 实例代码
 * .countWindow(3)
 * 	说明:哪个窗口先达到3个元素, 哪个窗口就关闭. 不影响其他的窗口.
 * 滑动窗口
 * 滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是window_size，一个是sliding_size。下面代码中的sliding_size设置为了2，也就是说，每收到两个相同key的数据就计算一次，每一次计算的window范围最多是3个元素。
 * 实例代码
 * .countWindow(3, 2)
 */
public class Flink01_Window_Count {
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
                //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))  // 定义长度为10s的会话窗口，每一个Key都有自己的窗口
                //.countWindow(3)  // 定义长度为3的基于个数的滚动窗口
                .countWindow(3, 2)  // 定义长度为3(窗口内元素的最大个数)，滑动步长为2的基于个数的滑动窗口
                .process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                            List<WaterSensor> list = AtguiguUtil.toList(elements);
                            out.collect("当前key: " + key + ", 数据量: " + list);  // ，每一个Key都有自己的窗口，只有在自己窗口满了3个元素之后，才会出发打印
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
