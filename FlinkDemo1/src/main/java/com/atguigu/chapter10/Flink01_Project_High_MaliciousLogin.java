package com.atguigu.chapter10;

import com.atguigu.bean.LoginEvent;
import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.jline.utils.Log;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月8日19:53:35
 * @contact: yuanxin9997@qq.com
 * @description:  第10章Flink CEP编程实战 - 恶意登录监控
 * 8.4恶意登录监控
 * 对于网站而言，用户登录并不是频繁的业务操作。如果一个用户短时间内频繁登录失败，就有可能是出现了程序的恶意攻击，比如密码暴力破解。
 * 因此我们考虑，应该对用户的登录失败动作进行统计，具体来说，如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示。这是电商网站、也是几乎所有网站风控的基本一环。
 * 8.4.1数据源
 * 文件: LoginLog.csv
 * 8.4.2封装数据的JavaBean类
 */
public class Flink01_Project_High_MaliciousLogin {
    public static void main(String[] Args) {
        System.out.println("Flink 流处理高阶编程实战");
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        KeyedStream<LoginEvent, Long> stream = env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\LoginLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new LoginEvent(
                            Long.parseLong(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000
                    );
                })
//                .keyBy(LoginEvent::getUserId)
//                .countWindow(2, 1)  // 基于个数的窗口
//                .process(new ProcessWindowFunction<LoginEvent, String, Long, GlobalWindow>() {  // 注意：目前做法不能解决时间乱序问题
//                    @Override
//                    public void process(Long userId,
//                                        ProcessWindowFunction<LoginEvent, String, Long, GlobalWindow>.Context ctx,
//                                        Iterable<LoginEvent> elements,
//                                        Collector<String> out) throws Exception {
//                        List<LoginEvent> list = AtguiguUtil.toList(elements);
//                        if (list.size()==2) {
//                            LoginEvent event1 = list.get(0);
//                            LoginEvent event2 = list.get(1);
//                            String type1 = event1.getEventType();
//                            String type2 = event2.getEventType();
//                            Long time1 = event1.getEventTime();
//                            Long time2 = event2.getEventTime();
//                            if ("fail".equals(type1) && "fail".equals(type2) && (Math.abs(time2 - time1) <= 2000)) {
//                                out.collect("用户 " + userId + " 在 " + event2.getEventTime() + " 时刻存在恶意登录风险");
//                            }
//                        }
//                    }
//                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((loginEvent, recordTimestamp) -> loginEvent.getEventTime())
                )
                .keyBy(LoginEvent::getUserId);

        // 1. 定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("fail2s")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.milliseconds(2001));

        // 2. 将模式作用到流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);

        // 3. 从模式流取出匹配到的数据
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.get("fail2s").get(0).getUserId() + "正在恶意登录...";
            }
        })
                .print()
                ;

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
