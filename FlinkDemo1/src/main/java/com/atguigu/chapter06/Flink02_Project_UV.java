package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: yuan.xin
 * @createTime: 2024年6月18日18:37:11
 * @contact: yuanxin9997@qq.com
 * @description: 第6章Flink流处理核心编程实战 网站独立访客数（UV）的统计  使用process
 *
6.1.2网站独立访客数（UV）的统计
上一个案例中，我们统计的是所有用户对页面的所有浏览行为，也就是说，同一用户的浏览行为会被重复统计。而在实际应用中，我们往往还会关注，到底有多少不同的用户访问了网站，所以另外一个统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）
准备数据
对于UserBehavior数据源来说，我们直接可以根据userId来区分不同的用户.
 */
public class Flink02_Project_UV {
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
                .filter(ub -> "pv".equals(ub.getBehavior()))
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, String>() {
                    Set<Long> userIdSet = new HashSet<>();
                    @Override
                    public void processElement(UserBehavior value,  // 输入数据
                                               KeyedProcessFunction<String, UserBehavior, String>.Context ctx,  // 上下文
                                               Collector<String> out) throws Exception {  // 输出数据
                        System.out.println(ctx.getCurrentKey());  // 输出当前key
                        boolean add = userIdSet.add(value.getUserId());
                        if (add) {  // 如果返回值为True，表示此次是新元素，False表示为旧元素
                            out.collect("uv的值是：" + userIdSet.size());
                        }
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
