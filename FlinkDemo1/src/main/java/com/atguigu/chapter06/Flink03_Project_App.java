package com.atguigu.chapter06;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/18 19:07
 * @contact: yuanxin9997@qq.com
 * @description: 场营销商业指标统计分析
 * <p>
 * 随着智能手机的普及，在如今的电商网站中已经有越来越多的用户来自移动端，相比起传统浏览器的登录方式，手机APP成为了更多用户访
 * 问电商网站的首选。对于电商企业来说，一般会通过各种不同的渠道对自己的APP进行市场推广，而这些渠道的统计数据（比如，不同网站
 * 上广告链接的点击量、APP下载量）就成了市场营销的重要商业指标。
 * 6.2.1APP市场推广统计 - 分渠道
 */
public class Flink03_Project_App {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        // 统计不同的渠道不同的行为的个数
        env
                .addSource(new AppSource())
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1L);
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

    // 自定义Source
    public static class AppSource implements SourceFunction<MarketingUserBehavior> {

        // 在这里把source读取的数据放入到流中
        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Random random = new Random();
            String[] behaviors = new String[]{"CLICK", "DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL"};
            String[] channels = new String[]{"APP_STORE", "XIAOMI_MARKET", "HUAWEI_MARKET", "QQ_MARKET", "WANDOUJIA"};
            Long timestamp = System.currentTimeMillis();
            while (true) {
                long userId = random.nextInt(2000) + 1;  // 生成1-2000的uid
                String behavior = behaviors[random.nextInt(behaviors.length)];  // 随机生成5种行为
                String channel = channels[random.nextInt(channels.length)];  // 随机生成5个渠道
                ctx.collect(new MarketingUserBehavior(
                        userId,
                        behavior,
                        channel,
                        timestamp
                ));
                Thread.sleep(200);
            }
        }

        // 取消source读取数据
        // 这个方法不会自动执行，可以在外面调用这个方法
        @Override
        public void cancel() {

        }
    }
}
