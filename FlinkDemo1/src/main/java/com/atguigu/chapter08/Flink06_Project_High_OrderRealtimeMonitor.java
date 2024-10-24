package com.atguigu.chapter08;

import com.atguigu.bean.LoginEvent;
import com.atguigu.bean.OrderEvent;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.awt.*;
import java.time.Duration;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月8日19:53:35
 * @contact: yuanxin9997@qq.com
 * @description:8.5订单支付实时监控
 * 在电商网站中，订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。
 * 对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。
 * 另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 *
 * todo: 使用Session窗口实现，此处为何不使用定时器
 */
public class Flink06_Project_High_OrderRealtimeMonitor {
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
        env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\OrderLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new OrderEvent(
                            Long.parseLong(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000
                    );
                })
                .assignTimestampsAndWatermarks(  // 添加水印
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((log, recordTimestamp) -> log.getEventTime())
                )
                .keyBy(order -> order.getOrderId())
                .window(EventTimeSessionWindows.withGap(Time.minutes(15)))  // 事件会话窗口
                .process(new ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>() {

                    private ValueState<OrderEvent> createState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                    }

                    @Override
                    public void process(Long orderId,
                                        ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>.Context context,
                                        Iterable<OrderEvent> elements,
                                        Collector<String> out) throws Exception {
                        List<OrderEvent> list = AtguiguUtil.toList(elements);
                        if (list.size()==2) {  // 窗口内有2个元素
                            System.out.println("订单：" + orderId + "正常创建和支付...");
                        }else{ // 窗口内只有1个元素
                            System.out.println(list.size());
                            // 判断窗口内的元素是create还是pay
                            // 如果是create，则更新状态
                            // 如果是pay，判断create状态中是否有值，如果有值，则表示超时支付，如果没值（没有create），表示只有pay没有create
                            OrderEvent event = list.get(0);
                            if ("create".equals(event.getEventType())) {
                                // 更新状态
                                createState.update(event);
                            }else{
                                if (createState.value() == null) {
                                    // Pay来的时候，没有create
                                    out.collect("订单：" + orderId + "只有pay，没有create");
                                }else{  // Pay来的时候，有create
                                    out.collect("订单：" + orderId + "超时支付");
                                }
                            }
                        }
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
