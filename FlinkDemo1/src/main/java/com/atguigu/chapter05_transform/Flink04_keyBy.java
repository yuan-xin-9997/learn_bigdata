package com.atguigu.chapter05_transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/05 20:20
 * @contact: yuanxin9997@qq.com
 * @description: Flink keyBy 算子
 */
public class Flink04_keyBy {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(2);

        // Flink程序主逻辑
        DataStreamSource<String> socketedTextStream = env.socketTextStream("hadoop102", 9999);
        // 需求：对奇数标识为1，偶数标识为0
        // keyBy算子处理过程：没有对数据发生任何变化
        socketedTextStream.keyBy(new KeySelector<String, Integer>() {
            @Override
            public Integer getKey(String value) throws Exception {
                if(Integer.parseInt(value) % 2 == 0){
                    return 0;
                }
                return 1;
            }
        })
                .print();

        // 结果
        // 如果并行度设置为2
        // 2> 1
        //2> 2
        //2> 3
        //2> 4
        //2> 5
        //2> 6
        //2> 7

        // 如果并行度设置为1
        // 1
        //2
        //34
        //5
        //6
        //7
        //

        // 如果并行度设置为16
        // 11> 1
        //12> 2
        //11> 3
        //12> 4
        //11> 5
        //12> 6
        //11> 7
        //12> 8
        //11> 9

        // > 前面的数字表示的是分区号（或channel号，或通道号）


        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
