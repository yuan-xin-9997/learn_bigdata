package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/26 21:11
 * @contact: yuanxin9997@qq.com
 * @description: 9.5 交易域订单预处理表
 *
 *  交易域订单预处理表
 * 9.5.1 主要任务
 * 经过分析，订单明细表和取消订单明细表的数据来源、表结构都相同，差别只在业务过程和过滤条件，为了减少重复计算，将两张表公共的关联过程提取出来，形成订单预处理表。
 * 关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表和字典表（维度业务表）形成订单预处理表，写入 Kafka 对应主题。
 * 本节形成的预处理表中要保留订单表的 type 和 old 字段，用于过滤订单明细数据和取消订单明细数据。
 *
 * order_info
 *          内连接 join
 *          ttl设置多少合理？
 *              下单回产生一条order_info和n条order_detail，几乎同时产生   ttl=10s即可
 *              订单取消，order_info一条数据发生update，order_detail不会发生任何变化
 *                  取消的时候，可能距离下单已经过去30分钟了，也需要去join以前的详情   ttl=1h
 * order_detail
 *          左连接 left join
 * activity
 *          左连接 left join
 * coupon
 *          lookup join
 * base_dic
 *
 * 写入kafka需要使用upsert-kafka
 */
public class Dwd05_DwdTradeOrderPreProcess extends BaseSqlApp {
    public static void main(String[] Args) {
        new Dwd05_DwdTradeOrderPreProcess().init(
                3005,
            2,
            "Dwd05_DwdTradeOrderPreProcess"
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {

        // 1. 读取ods_db
        readOdsDb(tEnv, "Dwd05_DwdTradeOrderPreProcess");

        // 2. 读取字典表
        readBaseDic(tEnv);

        // 3. 过滤出order_detail
        Table order_detail = tEnv.sqlQuery("select " +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['create_time'] create_time,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "data['sku_num'] sku_num,\n" +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
                "data['split_total_amount'] split_total_amount,\n" +
                "data['split_activity_amount'] split_activity_amount,\n" +
                "data['split_coupon_amount'] split_coupon_amount,\n" +
                "ts od_ts,\n" +
                "pt \n" +
                " from ods_db" +
                " where `database`='gmall2022' " +
                " and `table` = 'order_detail' " +
                " and `type`='insert' " +
                " ");
        tEnv.createTemporaryView("order_detail", order_detail);  // 注册临时表
        // order_detail.execute().print();

        // 4. 过滤出order_info
        Table orderInfo = tEnv.sqlQuery(" select " +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id,\n" +
                "data['operate_time'] operate_time,\n" +
                "data['order_status'] order_status,\n" +
                "`type`,\n" +
                "`old`,\n" +
                "ts oi_ts\n" +
                " from ods_db" +
                " where `database`='gmall2022' " +
                " and `table` = 'order_info' " +
                " and (`type`='insert' or `type`='update' )" +
                "");
        tEnv.createTemporaryView("order_info", orderInfo);  // 注册临时表
        orderInfo.execute().print();

        // 5. 过滤 活动表
        // 6. 过滤 优惠券使用情况
        // 7. 5张表join
        // 8. 定义动态表与输出的topic关联
        // 9. 将join结果写出到输出的表
    }
}
