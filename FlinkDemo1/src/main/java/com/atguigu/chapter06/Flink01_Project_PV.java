package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/17 21:48
 * @contact: yuanxin9997@qq.com
 * @description: 第6章Flink流处理核心编程实战 6.1基于埋点日志数据的网络流量统计
 *
 * 需求：
 * 6.1.1网站总浏览量（PV）的统计
 * 衡量网站流量一个最简单的指标，就是网站的页面浏览量（Page View，PV）。用户每次打开一个页面便记录1次PV，多次打开同一页面则浏览量累计。
 * 一般来说，PV与来访者的数量成正比，但是PV并不直接决定页面的真实来访者数量，如同一个来访者通过不断的刷新页面，也可以制
 * 造出非常高的PV。接下来我们就用咱们之前学习的Flink算子来实现PV的统计
 */
public class Flink01_Project_PV {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\UserBehavior.csv")
                .map(line->{
                    String[] data = line.split(",");
                    return new UserBehavior(Long.parseLong(
                            data[0]),
                            Long.parseLong(data[1]),
                            Integer.parseInt(data[2]),
                            data[3],
                            Long.parseLong(data[4]));
                })
                .filter(ub->"pv".equals(ub.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return Tuple2.of("pv", 1L);
                    }
                })
                .keyBy(t->t.f0)
                .sum(1)
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
