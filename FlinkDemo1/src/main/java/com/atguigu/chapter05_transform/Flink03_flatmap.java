package com.atguigu.chapter05_transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/04 21:46
 * @contact: yuanxin9997@qq.com
 * @description: Flink flatmap 算子:消费一个元素并产生零个或多个元素
 */
public class Flink03_flatmap {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // Flink程序主逻辑
        DataStreamSource<String> socketedTextStream = env.socketTextStream("hadoop102", 9999);
        // 常规写法
//        socketedTextStream.flatMap(new FlatMapFunction<String, Integer>() {
//            @Override
//            public void flatMap(String value, Collector<Integer> out) throws Exception {
//                int i = Integer.parseInt(value);
//                out.collect(i);
//                out.collect(i*i);
//                out.collect(i*i*i);
//            }
//        })
//                .print();
        // Lambda表达式写法
        socketedTextStream.flatMap((FlatMapFunction<String, Integer>) (value, out) -> {
            out.collect(Integer.parseInt(value));
            out.collect(Integer.parseInt(value)*Integer.parseInt(value));
            out.collect(Integer.parseInt(value)*Integer.parseInt(value)*Integer.parseInt(value));
        })
                .returns(Types.INT)
                .print();
        
        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
