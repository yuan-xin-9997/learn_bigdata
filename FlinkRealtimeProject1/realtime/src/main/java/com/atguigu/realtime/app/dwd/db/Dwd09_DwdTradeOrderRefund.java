package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024年8月27日19:44:19
 * @contact: yuanxin9997@qq.com
 * @description:9.9 交易域退单事务事实表
 * 9.9.1 主要任务
 * 从 Kafka 读取业务数据，筛选退单表数据，筛选满足条件的订单表数据，建立 MySQL-Lookup 字典表，关联三张表获得退单明细宽表。
 * 9.9.2 思路分析
 * 1）设置 ttl
 * 	用户执行一次退单操作时，order_refund_info 会插入多条数据，同时 order_info 表的一条对应数据会发生修改，所以两张表不存在业务上的时间滞后问题，因此仅考虑可能的乱序即可，ttl 设置为 5s。
 * 2）筛选退单表数据
 * 	退单业务过程最细粒度的操作为一个订单中一个 SKU 的退单操作，退单表粒度与最细粒度相同，将其作为主表。
 * 3）筛选订单表数据并转化为流
 * 	获取 province_id。退单操作发生时，订单表的 order_status 字段值会由1002（已支付）更新为 1005（退款中）。订单表中的数据要满足三个条件：
 * （1）order_status 为 1005（退款中）。
 * （2）操作类型为 update。
 * （3）更新的字段为 order_status。
 * 该字段发生变化时，变更数据中 old 字段下 order_status 的值不为 null（为 1002）。
 * 4）建立 MySQL-Lookup 字典表
 * 	获取退款类型名称和退款原因类型名称。
 * 5）关联这几张表获得退单明细宽表，写入 Kafka 退单明细主题
 * 	退单信息表 order_refund_info 的粒度为退单业务过程的最细粒度，将其作为主表。
 * 	（1）对单信息表与订单表的退单数据完全对应，不存在独有数据，通过内连接关联。
 * 	（2）与字典表通过内连接关联。
 * 第二步是否从订单表中筛选退单数据并不影响查询结果，提前对数据进行过滤是为了减少数据量，减少性能消耗。下文同理，不再赘述。
 */
public class Dwd09_DwdTradeOrderRefund extends BaseSqlApp {
    public static void main(String[] Args) {
        new Dwd09_DwdTradeOrderRefund().init(
                3009,
                2,
                "Dwd09_DwdTradeOrderRefund"
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {

        // 1. 读取ods_db
        readOdsDb(tEnv, "Dwd09_DwdTradeOrderRefund");

        // 2. 读取字典表
        readBaseDic(tEnv);

        // 3. 过滤退单表
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id,\n" +
                        "data['user_id'] user_id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['refund_type'] refund_type,\n" +
                        "data['refund_num'] refund_num,\n" +
                        "data['refund_amount'] refund_amount,\n" +
                        "data['refund_reason_type'] refund_reason_type,\n" +
                        "data['refund_reason_txt'] refund_reason_txt,\n" +
                        "data['create_time'] create_time,\n" +
                        "pt,\n" +
                        "ts\n" +
                        " from ods_db" +
                        " where `database`='gmall2022' " +
                        " and `table` = 'order_refund_info' " +
                        " and `type`='insert' " +
                        " ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        orderRefundInfo.execute().print();

        // 4. 过滤订单表
        Table orderInfo = tEnv.sqlQuery(
                "select " +

                        " from ods_db" +
                        " where `database`='gmall2022' " +
                        " and `table` = 'order_info' " +
                        " and `type`='update' " +
                        " and `old`['']" +
                        " ");

        // 5. join 退单表 订单表 字典表 3张

        // 6. 写出到kafka中


    }
}
