package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024年8月28日20:14:53
 * @contact: yuanxin9997@qq.com
 * @description:9.10 交易域退款成功事务事实表
 * 9.10.1 主要任务
 * （1）从退款表中提取退款成功数据，并将字典表的 dic_name 维度退化到表中。
 * （2）从订单表中提取退款成功订单数据。
 * （3）从退单表中提取退款成功的明细数据。
 * 9.10.2 思路分析
 * 1）设置 ttl
 * 	一次退款支付操作成功时，refund_payment 表会新增记录，订单表 order_info 和退单表order_refund_info 的对应数据会发生修改，几张表之间不存在业务上的时间滞后。与字典表的关联分析同上，不再赘述。因而，仅考虑可能的数据乱序即可。将 ttl 设置为 5s。
 * 1）建立 MySQL-Lookup 字典表
 * 	获取支付类型名称。
 * 2）读取退款表数据，筛选退款成功数据
 * 	退款表 refund_payment 的粒度为一个订单中一个 SKU 的退款记录，与退款业务过程的最细粒度相同，将其作为主表。
 * 	退款操作发生时，业务数据库的退款表会先插入一条数据，此时 refund_status 状态码应为 0701（商家审核中），callback_time 为 null，而后经历一系列业务过程：商家审核、买家发货、退单完成。退单完成时会将状态码由 0701 更新为 0705（退单完成），同时将 callback_time 更新为退款支付成功的回调时间。
 * 	由上述分析可知，退款成功记录应满足三个条件：（1）数据操作类型为 update；（2）refund_status 为 0705；（3）修改的字段包含 refund_status。
 * 3）读取订单表数据，过滤退款成功订单数据
 * 	用于获取 user_id 和 province_id。退款操作完后时，订单表的 order_status 字段会更新为 1006（退款完成），因此退单成功对应的订单数据应满足三个条件：（1）操作类型为 update；
 * （2）order_status 为 1006；（3）修改了 order_status 字段。
 * 	order_status 值更改为 1006 之后对应的订单表数据就不会再发生变化，所以只要满足前两个条件，第三个条件必定满足。
 * 4）筛选退款成功的退单明细数据
 * 	用于获取退单件数 refund_num。退单成功时 order_refund_info 表中的 refund_status 字段会修改为0705（退款成功状态码）。因此筛选条件有三：（1）操作类型为 update；（2）refund_status 为 0705；（3）修改了 refund_status 字段。筛选方式同上。
 * 5）关联四张表并写出到 Kafka 退款成功主题
 * 退款支付表的粒度为退款支付业务过程的最细粒度，即一个 sku 的退款操作，将其作为主表。
 * （1）退款支付表数据与订单表中的退款变更数据完全对应，不存在独有数据，内连接关联。
 * （2）退款支付数据与退单表退款变更数据完全对应，不存在独有数据，内连接关联。
 * （3）与字典表通过内连接关联。
 */
public class Dwd10_DwdTradeRefundPaySuc extends BaseSqlApp {
    public static void main(String[] Args) {
        new Dwd10_DwdTradeRefundPaySuc().init(
                3010,
                2,
                "Dwd10_DwdTradeRefundPaySuc"
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {

        // 1. 读取ods_db
        readOdsDb(tEnv, "Dwd09_DwdTradeOrderRefund");

        // 2. 读取字典表
        readBaseDic(tEnv);

        // 3. 过滤退款表
        Table orderRefundPayment = tEnv.sqlQuery(
                "select " +
                        "data['id'] id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['payment_type'] payment_type,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "data['total_amount'] total_amount,\n" +
                        "pt,\n" +
                        "ts\n" +
                        " from ods_db" +
                        " where `database`='gmall2022' " +
                        " and `table` = 'refund_payment' " +
                        // " and `type`='update' " +
                        // " and `old`['refund_status'] is not null " +
                        // " and `data`['refund_status'] = '0705' " +
                        " ");
        tEnv.createTemporaryView("refund_payment", orderRefundPayment);
        // orderRefundPayment.execute().print();

        // 4. 过滤退单表   最终结果需要用到refund_num
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['refund_num'] refund_num\n" +
                        " from ods_db" +
                        " where `database`='gmall2022' " +
                        " and `table` = 'order_refund_info' " +
                        // " and `type`='insert' " +
                        " ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 4. 过滤订单表
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        " data['id'] id,\n" +
                        " data['user_id'] user_id,\n" +
                        " data['province_id'] province_id,\n" +
                        " `old`\n" +
                        " from ods_db" +
                        " where `database`='gmall2022' " +
                        " and `table` = 'order_info' " +
                        " and `type`='update' " +
                        " and `old`['order_status'] is not null " +
                        " and `data`['order_status'] = '1006' " +
                        " ");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 5. join 退单表 订单表 字典表 3张
        Table result = tEnv.sqlQuery(
                "select " +
                        "rp.id,\n" +
                        "oi.user_id,\n" +
                        "rp.order_id,\n" +
                        "rp.sku_id,\n" +
                        "oi.province_id,\n" +
                        "rp.payment_type,\n" +
                        "dic.dic_name payment_type_name,\n" +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id,\n" +
                        "rp.callback_time,\n" +
                        "ri.refund_num,\n" +
                        "rp.total_amount,\n" +
                        "rp.ts \n" +
                        " from refund_payment rp " +
                        " join order_refund_info ri " +
                        " on rp.order_id = ri.order_id and rp.sku_id = ri.sku_id" +
                        " join order_info oi" +
                        " on rp.order_id = oi.id " +
                        " join base_dic for system_time as of rp.pt as dic " +
                        " on rp.payment_type=dic.dic_code "
        );

        // 6. 写出到kafka中
        tEnv.executeSql("create table dwd_trade_refund_pay_suc(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "date_id string,\n" +
                "callback_time string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts bigint\n" +
                // "row_op_ts timestamp_ltz(3)\n" +
                ")" + SQLUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC));

        result.executeInsert("dwd_trade_refund_pay_suc");
    }
}
