package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/21 21:09
 * @contact: yuanxin9997@qq.com
 * @description: 9.4 交易域加购事务事实表
 *
 * 9.4 交易域加购事务事实表
 * 9.4.1 主要任务
 * 提取加购操作生成加购表，并将字典表中的相关维度退化到加购表中，写出到 Kafka 对应主题。
 * 9.4.2 思路分析
 * 1）维度关联（维度退化）实现策略分析
 * 本章业务事实表的构建全部使用 FlinkSQL 实现，字典表数据存储在 MySQL 的业务数据库中，要做维度退化，就要将这些数据从 MySQL 中提取出来封装成 FlinkSQL 表，Flink 的 JDBC SQL Connector 可以实现我们的需求。
 * 2）知识储备
 * （1）JDBC SQL Connector
 * JDBC 连接器可以让 Flink 程序从拥有 JDBC 驱动的任意关系型数据库中读取数据或将数据写入数据库。
 * 如果在 Flink SQL 表的 DDL 语句中定义了主键，则会以 upsert 模式将流中数据写入数据库，此时流中可以存在 UPDATE/DElETE（更新/删除）类型的数据。否则，会以 append 模式将数据写出到数据库，此时流中只能有 INSERT（插入）类型的数据。
 * DDL 用法实例如下。
 * CREATE TABLE MyUserTable (
 *     id BIGINT,
 *     name STRING,
 *     age INT,
 *     status BOOLEAN,
 *     PRIMARY KEY (id) NOT ENFORCED
 * ) WITH (
 *     'connector' = 'jdbc',
 *     'url' = 'jdbc:mysql://localhost:3306/mydatabase',
 *     'table-name' = 'users'
 * );
 * （2）Lookup Cache
 * 	JDBC 连接器可以作为时态表关联中的查询数据源（又称维表）。目前，仅支持同步查询模式。
 * 	默认情况下，查询缓存（Lookup Cache）未被启用，需要设置 lookup.cache.max-rows 和 lookup.cache.ttl 参数来启用此功能。
 * 	Lookup 缓存是用来提升有 JDBC 连接器参与的时态关联性能的。默认情况下，缓存未启用，所有的请求会被发送到外部数据库。当缓存启用时，每个进程（即 TaskManager）维护一份缓存。收到请求时，Flink 会先查询缓存，如果缓存未命中才会向外部数据库发送请求，并用查询结果更新缓存。如果缓存中的记录条数达到了 lookup.cache.max-rows 规定的最大行数时将清除存活时间最久的记录。如果缓存中的记录存活时间超过了 lookup.cache.ttl 规定的最大存活时间，同样会被清除。
 * 	缓存中的记录未必是最新的，可以将 lookup.cache.ttl 设置为一个更小的值来获得时效性更好的数据，但这样做会增加发送到数据库的请求数量。所以需要在吞吐量和正确性之间寻求平衡。
 */
public class Dwd_04_DwdTradeCartAdd extends BaseSqlApp {
    public static void main(String[] Args) {
        new Dwd_04_DwdTradeCartAdd().init(3004, 2, "Dwd_04_DwdTradeCartAdd");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ODS_DB的数据
        // tEnv.executeSql("create table ods_db(" +
        //         " `database` string, " +
        //         " `table` string, " +
        //         " `type` string, " +
        //         " `ts` bigint, " +
        //         " `data` map<string, string>, " +
        //         " `old` map<string, string>, " +
        //         " `pt` as proctime() " +  // lookup join 的时候使用
        //         ") " + SQLUtil.getKafkaSource(Constant.TOPIC_ODS_DB, "Dwd_04_DwdTradeCartAdd"))
        //         ;
        readOdsDb(tEnv, "Dwd_04_DwdTradeCartAdd");
        // tEnv.sqlQuery("select * from ods_db")
        //         .execute()
        //         .print();

        // 2. 过滤出加购数据cart_info
        Table cartInfo = tEnv.sqlQuery("select " +
                // "`data`['id'] id," +
                // "`data`['user_id'] user_id," +
                // "`data`['sku_id'] sku_id," +
                // "source_id,\n" +
                // "source_type,\n" +
                // " if(`type`='insert', " +
                // " cast(`data`['sku_num'] as int),  " +
                // "  cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) ) sku_num " +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "if(`type` = 'insert',\n" +
                "data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num,\n" +
                "ts,\n" +
                "pt\n" +
                " from ods_db " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'cart_info' " +
                " and (" +
                " `type`='insert' " +
                "  or (`type`='update' " +
                "      and `old`['sku_num'] is not null" +
                "      and  cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) )" +  // 由于sku_num字段变大导致的update
                ") "
        );// .execute()
// .print()

        // 3.读取字典表
        readBaseDic(tEnv);

        // 4. 维度退化（cartInfo和字典表进行join）
        tEnv.createTemporaryView("cart_info", cartInfo);  // 注册临时表
        Table result = tEnv.sqlQuery("select " +
                "ci.id,\n" +
                "ci.user_id,\n" +
                "ci.sku_id,\n" +
                "ci.source_id,\n" +
                "ci.source_type,\n" +
                "dic.dic_name source_type_name,\n" +
                "ci.sku_num,\n" +
                "ci.ts\n" +
                " from cart_info ci " +
                " join base_dic for system_time as of ci.pt as dic" +  // lookup join 实现事实表和维度表join
                " on ci.source_type=dic.dic_code " +
                "");

        // result.execute().print();

        // 将结果写入到kafka
        // 5. 定义一个动态表与kafka的topic关联
        tEnv.executeSql("" +
                "create table dwd_trade_cart_add(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                // "ts string\n" +
                "ts bigint\n" +
                ") " + SQLUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );

        result.executeInsert("dwd_trade_cart_add");

        // QA:Exception in thread "main" org.apache.flink.table.api.ValidationException: Column types of query result and sink for registered table 'default_catalog.default_database.dwd_trade_cart_add' do not match.
        // Cause: Incompatible types for sink column 'ts' at position 7.


    }
}
























