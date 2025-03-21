package com.atguigu.realtime.common;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/31 20:25
 * @contact: yuanxin9997@qq.com
 * @description: 常量类
 */
public class Constant {
    public static final String KAFKA_BROKERS = "hadoop162:9092,hadoop163:9092,hadoop164:9092";
    public static final String TOPIC_ODS_DB = "ods_db";
    public static final String TOPIC_ODS_LOG = "ods_log";
    public static String PHENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static String PHENIX_URL = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL = "dwd_traffic_uv_detail";
    public static final String TOPIC_DWD_TRAFFIC_USER_JUMP_DETAIL = "dwd_traffic_uj_detail";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_PRE_PROCESS = "dwd_trade_order_pre_process";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_REFUND_PAY_SUC = "dwd_trade_refund_pay_suc";
    public static final String TOPIC_DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";
    public static final String DORIS_FENODES = "hadoop162:7030";
    public static final int TWO_DAY_SECONDS = 60 * 60 * 24 * 2;

    public static void main(String[] Args) {

    }
}
