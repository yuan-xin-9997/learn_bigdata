package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        stream.print();
    }
}
