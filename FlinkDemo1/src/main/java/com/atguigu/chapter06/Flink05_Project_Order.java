package com.atguigu.chapter06;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/18 19:50
 * @contact: yuanxin9997@qq.com
 * @description: 6.4订单支付实时监控
 *
 * 6.4订单支付实时监控
 * 在电商网站中，订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 * 需求: 来自两条流的订单交易匹配
 * 对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了。而往往这会来自不同的日志信息，所以我们要同时读入两条流的数据来做合并处理。
 * 数据准备
 * 订单数据从OrderLog.csv中读取，交易数据从ReceiptLog.csv中读取
 */
public class Flink05_Project_Order {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(fields[0]),
                            fields[1],
                            fields[2],
                            Long.valueOf(fields[3])
                    );
                })
                .filter(orderEvent -> "pay".equals(orderEvent.getEventType()));  // 对账需要，只需要支付记录

        SingleOutputStreamOperator<TxEvent> txEventStream = env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\ReceiptLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new TxEvent(
                            fields[0],
                            fields[1],
                            Long.valueOf(fields[2])
                    );
                });

        // 连接两条流，进行匹配
        orderEventStream.connect(txEventStream)
                .keyBy(OrderEvent::getTxId, TxEvent::getTxId)
                .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                    Map<String, TxEvent> txEventMap = new HashMap<>();
                    Map<String, OrderEvent> orderEventMap = new HashMap<>();
                    // 处理第一条流的数据
                    @Override
                    public void processElement1(OrderEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        // 支付记录先来，查询txEventMap 是否有对应的流水记录，如果有对账成功，如果没有将自己存到orderEventMap
                        TxEvent txEvent = txEventMap.get(ctx.getCurrentKey());
                        if (txEvent != null) {
                            out.collect("订单：" + value.getOrderId() + "对账成功，流水号：" + txEvent.getTxId());
                        }else{
                            orderEventMap.put(ctx.getCurrentKey(), value);
                        }
                    }
                    // 处理第二条流的数据
                    @Override
                    public void processElement2(TxEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        OrderEvent orderEvent = orderEventMap.get(ctx.getCurrentKey());
                        if (orderEvent != null) {
                            out.collect("订单：" + orderEvent + "对账成功，流水号：" + ctx.getCurrentKey());
                        }else{
                            txEventMap.put(ctx.getCurrentKey(), value);
                        }
                    }
                })
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
