package com.atguigu.chapter05_transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/04 21:33
 * @contact: yuanxin9997@qq.com
 * @description: Flink filter 算子
 */
public class Flink02_filter {
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
//        socketedTextStream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String value) throws Exception {
//                try {
//                    if(Integer.parseInt(value) % 2 == 0){
//                        return false;
//                    }
//                    return true;
//                } catch (NumberFormatException e) {
//                    return false;
//                }
//            }
//        })
//                .print();
        // Lambda表达式写法
        socketedTextStream.filter(value -> Integer.parseInt(value) % 2 != 0)
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
