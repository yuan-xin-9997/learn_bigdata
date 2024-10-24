package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradePaymentWindowBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: yuan.xin
 * @createTime: 2024/10/23 20:55
 * @contact: yuanxin9997@qq.com
 * @description: 10.7 交易域支付各窗口汇总表（练习）
 *
 * 10.7.1 主要任务
 * 从 Kafka 读取交易域支付成功主题数据，统计支付成功独立用户数和首次支付成功用户数。
 * 10.7.2 思路分析
 * 1）从 Kafka 支付成功明细主题读取数据
 * 2）转换数据结构
 * 	String 转换为 JSONObject。
 * 3）设置水位线，按照 user_id 分组
 * 4）统计独立支付人数和新增支付人数
 * 	运用 Flink 状态编程，在状态中维护用户末次支付日期。
 * 	若末次支付日期为 null，则将首次支付用户数和支付独立用户数均置为 1；否则首次支付用户数置为 0，判断末次支付日期是否为当日，如果不是当日则支付独立用户数置为 1，否则置为 0。最后将状态中的支付日期更新为当日。
 * 5）开窗、聚合
 * 度量字段求和，补充窗口起始时间、结束时间和当天日期字段。
 * 6）写出到 Doris
 * 10.7.3 图解
 *
 * 10.7.4 Doris建表语句
 * drop table if exists dws_trade_payment_suc_window;
 * create table if not exists dws_trade_payment_suc_window
 * (
 *     `stt`                           DATETIME comment '窗口起始时间',
 *     `edt`                           DATETIME comment '窗口结束时间',
 *     `cur_date`                      DATE comment '当天日期',
 *     `payment_suc_unique_user_count` BIGINT replace comment '支付成功独立用户数',
 *     `payment_new_user_count`        BIGINT replace comment '支付成功新用户数'
 * ) engine = olap aggregate key (`stt`, `edt`, `cur_date`)
 * comment "交易域加购各窗口汇总表"
 * partition by range(`cur_date`)()
 * distributed by hash(`stt`) buckets 10 properties (
 *   "replication_num" = "3",
 *   "dynamic_partition.enable" = "true",
 *   "dynamic_partition.time_unit" = "DAY",
 *   "dynamic_partition.start" = "-1",
 *   "dynamic_partition.end" = "1",
 *   "dynamic_partition.prefix" = "par",
 *   "dynamic_partition.buckets" = "10",
 *   "dynamic_partition.hot_partition_num" = "1"
 */
public class Dws07_DwsTradePaymentSucWindow extends BaseAppV1 {
    public static void main(String[] Args) {
        /**
         * 本app需要提前启动的app
         *  1.预处理app Dwd05_DwdTradeOrderPreProcess
         *  2.下单详情 Dwd06_DwdTradeOrderDetail
         *  3.支付成功明细 Dwd08_DwdTradePayDetailSuc
         */
        new Dws07_DwsTradePaymentSucWindow().init(40007, 2,
                "Dws07_DwsTradePaymentSucWindow",
                Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // stream.print();
        stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {

                    private ValueState<String> lastPaySucDateState;  // 当前用户最后一次支付成功的日期

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastPaySucDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastPaySucDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>.Context ctx,
                                               Collector<TradePaymentWindowBean> out) throws Exception {
                        String lastPaySucDate = this.lastPaySucDateState.value();
                        long ts = obj.getLong("ts") * 1000;
                        String today = AtguiguUtil.toDate(ts);
                        // 支付成功独立用户数
                        Long paymentSucUniqueUserCount = 0L;
                        // 支付成功新用户数
                        Long paymentSucNewUserCount = 0L;
                        if (!today.equals(lastPaySucDate)) {
                            // 这个用户今天第一次支付成功
                            paymentSucUniqueUserCount = 1L;
                            lastPaySucDateState.update(today);
                            // 是否首次支付（是否新支付用户）
                            if (lastPaySucDate == null) {
                                paymentSucNewUserCount = 1L;
                            }
                        }
                        if (paymentSucUniqueUserCount==1) {
                            out.collect(new TradePaymentWindowBean(
                                    "","","",
                                    paymentSucUniqueUserCount,
                                    paymentSucNewUserCount,
                                    ts
                            ));
                        }
                    }
                })
                // .print()
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradePaymentWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                            @Override
                            public TradePaymentWindowBean reduce(TradePaymentWindowBean bean1,
                                                                 TradePaymentWindowBean bean2) throws Exception {
                                bean1.setPaymentSucUniqueUserCount(bean1.getPaymentSucUniqueUserCount() + bean2.getPaymentSucUniqueUserCount());
                                bean1.setPaymentNewUserCount(bean1.getPaymentNewUserCount() + bean2.getPaymentNewUserCount());
                                return bean1;
                            }
                        },
                        new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window,
                                              Iterable<TradePaymentWindowBean> values,
                                              Collector<TradePaymentWindowBean> out) throws Exception {
                                TradePaymentWindowBean bean = values.iterator().next();
                                bean.setStt(AtguiguUtil.toDateTime(window.getStart()));
                                bean.setEdt(AtguiguUtil.toDateTime(window.getEnd()));
                                bean.setCurDate(AtguiguUtil.toDateTime(System.currentTimeMillis()));
                                out.collect(bean);
                            }
                        }
                )
                .map(bean->{
                    SerializeConfig config = new SerializeConfig();
                    config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
                    return JSON.toJSONString(bean, config);
                })
                .addSink(FlinkSinkUtil.getDoriSink(
                        "gmall2022.dws_trade_payment_suc_window"
                ))
                ;
    }
}




























