package com.atguigu.chapter10;

import com.atguigu.bean.LoginEvent;
import com.atguigu.bean.OrderEvent;
import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.*;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author: yuan.xin
 * @createTime: 2024年7月8日19:53:35
 * @contact: yuanxin9997@qq.com
 * @description: 第10章Flink CEP编程实战 - 8.5订单支付实时监控
 * 在电商网站中，订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。
 * 对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。
 * 另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 *
 * 9.4.6匹配后跳过策略
 * 对于一个给定的模式，同一个事件可能会分配到多个成功的匹配上。为了控制一个事件会分配到多少个匹配上，你需要指定跳过策略AfterMatchSkipStrategy。 有五种跳过策略，如下：
 * NO_SKIP: 每个成功的匹配都会被输出。
 * SKIP_TO_NEXT: 丢弃以相同事件开始的所有部分匹配。
 * SKIP_PAST_LAST_EVENT: 丢弃起始在这个匹配的开始和结束之间的所有部分匹配。
 * SKIP_TO_FIRST: 丢弃起始在这个匹配的开始和第一个出现的名称为PatternName事件之间的所有部分匹配。
 * SKIP_TO_LAST: 丢弃起始在这个匹配的开始和最后一个出现的名称为PatternName事件之间的所有部分匹配。
 *
 * AfterMatchSkipStrategy skipStrategy = ...
 * Pattern.begin("patternName", skipStrategy);
 *
 */
public class Flink02_Project_High_OrderRealtimeMonitor {
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
        KeyedStream<OrderEvent, Long> stream = env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\OrderLog.csv")
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
                .keyBy(order -> order.getOrderId());//                .window(EventTimeSessionWindows.withGap(Time.minutes(15)))  // 事件会话窗口
//                .process(new ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>() {
//
//                    private ValueState<OrderEvent> createState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
//                    }
//
//                    @Override
//                    public void process(Long orderId,
//                                        ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>.Context context,
//                                        Iterable<OrderEvent> elements,
//                                        Collector<String> out) throws Exception {
//                        List<OrderEvent> list = AtguiguUtil.toList(elements);
//                        if (list.size()==2) {  // 窗口内有2个元素
//                            System.out.println("订单：" + orderId + "正常创建和支付...");
//                        }else{ // 窗口内只有1个元素
//                            System.out.println(list.size());
//                            // 判断窗口内的元素是create还是pay
//                            // 如果是create，则更新状态
//                            // 如果是pay，判断create状态中是否有值，如果有值，则表示超时支付，如果没值（没有create），表示只有pay没有create
//                            OrderEvent event = list.get(0);
//                            if ("create".equals(event.getEventType())) {
//                                // 更新状态
//                                createState.update(event);
//                            }else{
//                                if (createState.value() == null) {
//                                    // Pay来的时候，没有create
//                                    out.collect("订单：" + orderId + "只有pay，没有create");
//                                }else{  // Pay来的时候，有create
//                                    out.collect("订单：" + orderId + "超时支付");
//                                }
//                            }
//                        }
//                    }
//                }).print();

        // 定义模式
        // 目标：1.只有pay 2.create和pay超时

        // 匹配成功的是正常数据
        // 1. 如果没有pay或者pay超过create 30分钟，create会进入超时流
        // 2. 只有pay，没有create 怎么办
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create",
                        AfterMatchSkipStrategy.skipPastLastEvent()  // 匹配后跳过策略
                )
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .optional()
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(30));

        // 3. 把规则作用到流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(stream, pattern);

        // 4. 从模式流中选择出匹配的数据
//        SingleOutputStreamOperator<String> normal = patternStream
//                .select(
//                        new OutputTag<String>("timeout") {
//                        },
//                        new PatternTimeoutFunction<OrderEvent, String>() {  // 超时的结果
//                            @Override
//                            public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
//                                return pattern.get("create").get(0).getOrderId() + " 超时未支付";
//                            }
//                        },
//                        new PatternSelectFunction<OrderEvent, String>() {  // 匹配成功的结果
//                            @Override
//                            public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
//                                return pattern.toString();
//                            }
//                        }
//                );
        SingleOutputStreamOperator<OrderEvent> normal = patternStream
                .flatSelect(
                        new OutputTag<OrderEvent>("timeout") {
                        },
                        new PatternFlatTimeoutFunction<OrderEvent, OrderEvent>() {
                            @Override
                            public void timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp, Collector<OrderEvent> out) throws Exception {
                                OrderEvent orderEvent = pattern.get("create").get(0);
                                out.collect(orderEvent);
                            }
                        },
                        new PatternFlatSelectFunction<OrderEvent, OrderEvent>() {
                            @Override
                            public void flatSelect(Map<String, List<OrderEvent>> pattern, Collector<OrderEvent> out) throws Exception {
                                if (!pattern.containsKey("create")) {  // 只有pay
                                    out.collect(pattern.get("pay").get(0));
                                }
                            }
                        }
                );

        // 5. 打印正常流和超时流
//        normal.print("正常流");
//        normal.getSideOutput(new OutputTag<OrderEvent>("timeout"){}).print("超时流");
        normal.getSideOutput(new OutputTag<OrderEvent>("timeout"){}).union(normal)
                .keyBy(OrderEvent::getOrderId)
                .process(new KeyedProcessFunction<Long, OrderEvent, String>() {

                    private ValueState<OrderEvent> createState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                    }

                    @Override
                    public void processElement(OrderEvent value, KeyedProcessFunction<Long, OrderEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        if ("create".equals(value.getEventType())) {
                            createState.update(value);
                        }else{
                            if (createState.value() == null) {
                                out.collect(value.getOrderId() + " 只有pay没有create");
                            }else{
                                out.collect(value.getOrderId() + " 被超时支付");
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
