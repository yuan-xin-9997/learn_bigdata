package com.atguigu.chapter12;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/22 19:36
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL 实现 TopN问题
 *
 * 第12章Flink SQL编程实战
 * 12.1使用SQL实现热门商品TOP N
 * 目前仅 Blink 计划器支持 Top-N 。
 * Flink 使用 OVER 窗口条件和过滤条件相结合以进行 Top-N 查询。利用 OVER 窗口的 PARTITION BY 子句的功能，Flink 还支持逐组 Top-N 。 例如，每个类别中实时销量最高的前五种产品。批处理表和流处理表都支持基于SQL的 Top-N 查询。
 * 流处理模式需注意:  TopN 查询的结果会带有更新。 Flink SQL 会根据排序键对输入的流进行排序；若 top N 的记录发生了变化，变化的部分会以撤销、更新记录的形式发送到下游。 推荐使用一个支持更新的存储作为 Top-N 查询的 sink 。另外，若 top N 记录需要存储到外部存储，则结果表需要拥有与 Top-N 查询相同的唯一键。
 * 12.1.1需求描述
 * 每隔30min 统计最近 1hour的热门商品 top3, 并把统计的结果写入到mysql中
 * 思路:
 * 1.统计每个商品的点击量, 开窗
 * 2.分组窗口分组,
 * 3.over窗口
 * 12.1.2数据源
 * input/UserBehavior.csv
 * 12.1.3在mysql中创建表
 * CREATE DATABASE flink_sql;
 * USE flink_sql;
 * DROP TABLE IF EXISTS `hot_item`;
 * CREATE TABLE `hot_item` (
 *   `w_end` timestamp NOT NULL,
 *   `item_id` bigint(20) NOT NULL,
 *   `item_count` bigint(20) NOT NULL,
 *   `rk` bigint(20) NOT NULL,
 *   PRIMARY KEY (`w_end`,`rk`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * 12.1.4导入JDBC Connector依赖
 * <dependency>
 *     <groupId>org.apache.flink</groupId>
 *     <artifactId>flink-connector-jdbc_${scala.binary.version}</artifactId>
 *     <version>${flink.version}</version>
 *
 *
 */
public class TopN {
    public static void main(String[] Args) {
        // 设置环境变量
        setProperty("HADOOP_USER_NAME", "atguigu");

        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑

        // SQL 执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 建立1个动态表与文件进行关联
        tEnv.executeSql("create table userBehavior(" +
                "    user_id bigint, " +
                "    item_id bigint, " +
                "    category_id bigint, " +
                "    behavior string, " +
                "    ts bigint, " +
                "    et as to_timestamp_ltz(ts, 0), " +
                "    watermark for et as et - interval '3' second " +
                " ) " +
                " with (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'FlinkDemo1/input/UserBehavior.csv', " +
                " 'format' = 'csv' " +
                " ) ");
        //tEnv.sqlQuery("select * from userBehavior").execute().print();

        // 2. 过滤出需要的数据：pv
        // tEnv.sqlQuery("select item_id,et from userBehavior where behavior = 'pv'").execute().print();
        Table t1 = tEnv.sqlQuery("select item_id,et from userBehavior where behavior = 'pv'");
        tEnv.createTemporaryView("t1", t1);

        // 3. 开窗聚合：统计每个商品在每个窗口内的点击量
        Table t2 = tEnv.sqlQuery("select " +
                " window_start,window_end,item_id," +
                " count(*) ct " +  // count(1) count(*)  count(id) sum(1)
                " from table(tumble(table t1,descriptor(et), interval '1' hour) ) " +
                " group by window_start,window_end,item_id "
        )
//                .execute().print()
                ;
        tEnv.createTemporaryView("t2", t2);

        // 4. 使用over窗口函数开窗，窗口函数为row_number
        Table t3 = tEnv
                .sqlQuery(" select" +
                        " window_start,window_end,item_id, ct, " +
                        //" row_number() over(partition by window_end order by ct desc) " +  // The window can only be ordered in ASCENDING mode.
                        " row_number() over(partition by window_end order by ct desc) rn " +
                        " from t2")
//                .execute()
//                .print()
        ;
        tEnv.createTemporaryView("t3", t3);


        // 5. 根据窗口和商品分组，并限制每组数据最多输出N条(过滤rn<=3)
        Table result = tEnv.sqlQuery("select" +
                " * " +
                " from t3 " +
                " where rn <= 3 ")   // 注意：此处是特殊用法，必须要这么写，blink专门为topN的实现
//                .execute()
//                .print()
        ;

        // 6. 把结果写出到支持更新的sink中（MySQL）
        // mysql建表
        // CREATE DATABASE flink_sql;
        //USE flink_sql;
        //DROP TABLE IF EXISTS `hot_item`;
        //CREATE TABLE `hot_item` (
        //  `w_end` timestamp NOT NULL,
        //  `item_id` bigint(20) NOT NULL,
        //  `item_count` bigint(20) NOT NULL,
        //  `rk` bigint(20) NOT NULL,
        //  PRIMARY KEY (`w_end`,`rk`)
        //) ENGINE=InnoDB DEFAULT CHARSET=utf8;

        // 建立Flink动态表到MySQL表的关联
        tEnv.executeSql("CREATE TABLE `hot_item` (" +
                "  `w_end` timestamp," +
                "  `item_id` bigint," +
                "  `item_count` bigint," +
                "  `rk` bigint," +
                "  PRIMARY KEY (`w_end`,`rk`) not enforced"  +  // Flink 没有主键，不支持对主键进行校验
                ")" +
                "WITH (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:mysql://hadoop102:3306/flink_sql'," +
                "   'table-name' = 'hot_item', " +
                "   'username' = 'root', " +
                "   'password' = 'root' " +
                ")"

        )
                ;
        tEnv.executeSql(" insert into hot_item select window_end w_end,item_id, ct item_count,rn rk from " + result);

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
