package com.atguigu.chapter07.watermark;

import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/20 19:23
 * @contact: yuanxin9997@qq.com
 * @description: Flink 水印  侧输出流
 *
 * 侧输出流实现分流功能
 *
 * 7.5.2使用侧输出流把一个流拆成多个流
 * split算子可以把一个流分成两个流, 从1.12开始已经被移除了. 官方建议我们用侧输出流来替换split算子的功能.
 * 需求: 采集监控传感器水位值，将水位值高于5cm的值输出到side output
 */
public class Flink06_sideOut_1 {
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
                .process(new ProcessFunction<WaterSensor, String>() {
                    // 分流必须写在process算子中
                    @Override
                    public void processElement(WaterSensor value,
                                               ProcessFunction<WaterSensor, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        if ("sensor_1".equals(value.getId())) {
                            out.collect(value.toString());  // 主流
                        }else if ("sensor_2".equals(value.getId())){
                            ctx.output(new OutputTag<String>("s2"){}, value.toString());  // 支流
                        }else {
                            ctx.output(new OutputTag<String>("other"){}, value.toString());  // 支流
                        }
                    }
                });

        main.print("main");
        main.getSideOutput(new OutputTag<WaterSensor>("s2"){}).print("s2");
        main.getSideOutput(new OutputTag<WaterSensor>("other"){}).print("other");

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
