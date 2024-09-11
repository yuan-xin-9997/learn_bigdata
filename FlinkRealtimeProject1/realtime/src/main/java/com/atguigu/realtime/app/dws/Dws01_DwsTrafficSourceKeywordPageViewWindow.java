package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.function.IKAnalyzer;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/09/11 19:14
 * @contact: yuanxin9997@qq.com
 * @description: 10.1 流量域来源关键词粒度页面浏览各窗口汇总表
 *
 * 需要启动yarn session： bin/yarn-session.sh -d
 * 启动日志分流任务：com.atguigu.realtime.app.dwd.log.Dwd01_DwdBaseLogApp  在 /opt/gmall/realtime.sh
 *
 * ----------------------------------------------------------------------------------
 *
 * 10.1.1 主要任务
 * 	从 Kafka 页面浏览明细主题读取数据，过滤搜索行为，使用自定义 UDTF（一进多出）函数对搜索内容分词。统计各窗口各关键词出现频次，写入 Doris。
 * 10.1.2 思路分析
 * 本程序将使用 FlinkSQL 实现。分词是个一进多出的过程，需要一个 UDTF 函数来实现，FlinkSQL 没有提供相关的内置函数，所以要自定义 UDTF 函数。
 * 	自定义函数的逻辑在代码中实现，要完成分词功能，需要导入相关依赖，此处将借助 IK 分词器完成分词。
 * 	最终要将数据写入 Doris，需要补充相关依赖，相关子类和工具类中的方法。本节任务分为两部分：分词处理和数据写出。
 * 1）分词处理
 * 分词处理分为八个步骤，如下。
 * （1）创建分词工具类
 * 	定义分词方法，借助 IK 分词器提供的工具将输入的关键词拆分成多个词，返回一个 List 集合。
 * （2）创建自定义函数类
 * 	继承 Flink 的 TableFunction 类，调用分词工具类的分词方法，实现分词逻辑。
 * （3）注册函数
 * （4）从 Kafka 页面浏览明细主题读取数据并设置水位线
 * （5）过滤搜索行为
 * 	满足以下三个条件的即为搜索行为数据：
 * ① page 字段下 item 字段不为 null；
 * ② page 字段下 last_page_id 为 search；
 * ③ page 字段下 item_type 为 keyword。
 * （6）分词
 * （7）分组、开窗、聚合计算
 * 按照拆分后的关键词分组。统计每个词的出现频次，补充窗口起始时间、结束时间、关键词来源（source）和当天日期（用于分区）字段。
 * （8）将动态表转换为流
 * 2）将数据写入 Doris
 * 	（1）建表
 * 	① Doris 建表首先要明确表引擎和数据模型，本项目没有特殊需求，表引擎使用默认的 olap 即可，接下来考虑数据模型。Flink 程序可以通过异步分界线快照算法实现精准一次语义，前提有二。
 * 一是 Source 端是可以重置读取位置的数据源。本项目 Source 端使用了 Kafka，满足此要求。
 * 二是 Sink 端需要实现事务，或者支持幂等写入。如果实现事务，程序故障重启时可以回滚清除未被状态后端记录的数据，从而保证精准一次。为了实现事务，我们需要继承抽象类 TwoPhaseCommitSinkFunction，重写抽象方法，实现过程较为繁琐。如果 Sink 端是支持幂等写入的数据库，可以一定程度上简化程序。Doris 提供了幂等写入，可以通过 Aggregate 模型的 replace 方式实现，也可以用 Unique 模型实现，二者的内部实现方式和数据存储方式完全一致。此处选用 Aggregate 模型的 replace 方式。
 * 	② 分区
 * 	分区可以在查询数据时帮助我们避免全表扫描，提升查询效率。本项目将会聚合 OLAP 数据库中的当日数据并将结果通过 SringBoot 服务传递给前端页面，进行可视化展示。显然，按天分区最为合理。
 * Doris 有两种分区方式：Range Partition 和 List Partition，前者通过划分区间实现分区，后者通过枚举的方式实现分区，按天分区显然是在划分时间范围，应选用 Range Partition。
 * 考虑下一个问题，Range Partition 的分区划分通过 less than 语法完成，用户需要定期创建未来分区，删除历史分区，开发人员需要耗费大量精力维护。如果能把这些工作交给 Doris 来完成可以大大降低维护成本，Doris 的动态分区可以实现这样的需求。动态分区可以实现表级分区的生命周期管理，建表时设定动态分区规则，FE 会启动一个后台线程，根据建表时设定的规则创建或删除分区，降低维护成本。动态分区及表级部分配置项解读如下。
 * dynamic_partition.enable：是否开启动态分区特性，可指定true或false,默认为true
 * dynamic_partition.time_unit：动态分区调度的单位，可指定HOUR、DAY、WEEK、MONTH。本项目按天分区，指定为 DAY，后缀格式为 yyyyMMdd。
 * dynamic_partition.start：动态分区的起始偏移，为负数。根据 time_unit 属性的不同，以当天（星期/月）为基准，分区范围在此偏移之前的分区将会被删除。如果不填写默认值为Interger.Min_VALUE 即-2147483648，即不删除历史分区。通常实时项目不需要保留历史数据，理论上仅保留当日数据即可，但考虑这样一种情况，数仓上线一段时间后，对零点前后生成的数据做运算，可能系统时间已进入第二天，而第一天的分区数据计算仍未完成，此时就有必要保留前一日分区。因此将其设为-1，仅保留昨日及之后的数据。
 * dynamic_partition.end：动态分区的结束偏移，为正数。根据 time_unit 属性的不同，以当天（星期/月）为基准，提前创建对应范围的分区。本项目按天分区，提前一日创建分区即可，该参数设为 1，避免数据写入时分区不存在而报错。
 * dynamic_partition.prefix：动态创建的分区名前缀，此处设为 par。
 * dynamic_partition.buckets：动态创建的分区所对应分桶数量。分桶数越多，单次查询就可以定位到更加精确的数据集，查询效率更高，但同时数据会更加分散，可能会导致大量小文件的产生，分桶数应在二者之间寻找平衡，生产环境需要结合实际情况调整。最佳分桶数的确定属于项目调优内容，本文不过多介绍，此处设置为 10。
 * dynamic_partition.replication_num：动态创建的分区所对应的副本数量，如果不填写，则默认为该表创建时指定的副本数量。此处不设该参数，副本数由 replication_num 参数指定。
 * dynamic_partition.hot_partition_num：指定最新的多少个分区为热分区。对于热分区，系统会自动设置其 storage_medium 参数为SSD，并且设置 storage_cooldown_time。上文提到，对零点前后生成数据的运算可能会有昨日分区数据的参与，为了不影响效率，昨日分区同样优先使用 SSD 介质存储，该参数设置为 1。
 * dynamic_partition.create_history_partition：默认为 false。当置为 true 时，Doris 会自动创建所有历史分区，当期望创建的分区个数大于 max_dynamic_partition_num 值时，操作将被禁止。当不指定 start 属性时，该参数不生效。实时数仓不对历史数据做处理，无须创建历史分区，因此该参数保持默认值即可。
 * replication_num：指定表副本数，副本数不可超过 IP 互不相同的 BE 节点数量。本项目 BE 分布在三台节点，副本数最大为 3。该参数设置为 3。
 * 	（2）写出方式
 * 	Doris 官方提供了 Flink Doris Connector 可以实现 Flink SQL 及 Flink 流处理与 Doris 的数据交互。本项目的表均为分区表，在向分区表写入数据时要指定分区，SQL 和流处理方式均可通过 sink.properties.partition 参数指定数据分区。分区信息包含在 Flink 流的数据中，我们无法将其取出传递给 sink.properties.partition 参数，因此 Flink Doris Connector 不能解决本项目需求。此处选择继承 RichSinkFunction，重写抽象方法，在其中自定义数据写出逻辑。在自定义的 RickSinkFunction 子类中，获取每条数据的分区名称，拼接 DML 语句，通过 JDBC 方式连接 Doris 将数据写出。
 * 	（3）TransientSink
 * 	在实体类中某些字段是为了辅助指标计算而设置的，并不会写入到数据库。那么，如何告诉程序哪些字段不需要写入数据库呢？Java 的反射提供了解决思路。类的属性对象 Field 可以调用 getAnnotation(Class annotationClass) 方法获取写在类中属性定义语句上方的注解中的信息，若注解存在则返回值不为 null。
 * 	定义一个可以写在属性上的注解，对于不需要写入数据库的属性，在实体类中属性定义语句上方添加该注解。为数据库操作对象传参时判断注解是否存在，是则跳过属性，即可实现对属性的排除。
 * 10.1.3 图解
 *
 * 10.1.4 Doris建表语句
 * drop table if exists dws_traffic_source_keyword_page_view_window;
 * create table if not exists dws_traffic_source_keyword_page_view_window
 * (
 *     `stt`           DATETIME comment '窗口起始时间',
 *     `edt`           DATETIME comment '窗口结束时间',
 *     `source`        VARCHAR(10) comment '关键词来源',
 *     `keyword`       VARCHAR(10) comment '关键词',
 *     `cur_date`      DATE comment '当天日期',
 *     `keyword_count` BIGINT replace comment '关键词评分'
 * ) engine = olap aggregate key (`stt`, `edt`, `source`, `keyword`, `cur_date`)
 * comment "流量域来源-关键词粒度页面浏览汇总表"
 * partition by range(`cur_date`)()
 * distributed by hash(`keyword`) buckets 10 properties (
 *   "replication_num" = "3",
 *   "dynamic_partition.enable" = "true",
 *   "dynamic_partition.time_unit" = "DAY",
 *   "dynamic_partition.start" = "-1",
 *   "dynamic_partition.end" = "1",
 *   "dynamic_partition.prefix" = "par",
 *   "dynamic_partition.buckets" = "10",
 *   "dynamic_partition.hot_partition_num" = "1"
 * );
 */
public class Dws01_DwsTrafficSourceKeywordPageViewWindow extends BaseSqlApp {
    public static void main(String[] Args) {
        new Dws01_DwsTrafficSourceKeywordPageViewWindow().init(
                4002,
                2,
                "Dws01_DwsTrafficSourceKeywordPageViewWindow"
        );

    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 创建动态表与topic关联：page
        tEnv.executeSql("create table page(" +
                " page map<string, string>, " +
                " ts bigint," +
                " et as to_timestamp_ltz(ts, 3)," +
                " watermark for et as et - interval '3' second " +
                ") " + SQLUtil.getKafkaSource(
                Constant.TOPIC_DWD_TRAFFIC_PAGE,
                "Dws01_DwsTrafficSourceKeywordPageViewWindow"
                ))
                ;

        // 2. 过滤出关键词
        // tEnv
        //         .sqlQuery("select * from page")
        //         .execute()
                // .print()
        Table keywordTable = tEnv
                .sqlQuery("select " +
                        " page['item']  keyword, " +
                        " et " +
                        " from page " +
                        " where page['last_page_id']='search' " +
                        " and page['item_type']='keyword' " +
                        " and page['item'] is not null ");
        // keywordTable.execute().print();
        tEnv.createTemporaryView("keyword_table" ,  keywordTable);

        // 3. 对关键词进行分词
        // 自定义函数：标量 制表 聚合 制表聚合
        tEnv.createTemporaryFunction("ik_analyzer", IKAnalyzer.class);
        Table kwTable = tEnv.sqlQuery(
                "select " +
                        " kw," +
                        " et" +
                        " from " +
                        " keyword_table" +
                        " join lateral table (ik_analyzer(keyword)) " +   // 侧窗
                        " on true"
        );// .execute()
// .print()
        tEnv.createTemporaryView("kw_table" ,  kwTable);

        // 4. 对分词后的词，开窗聚合统计次数
        Table result = tEnv
                .sqlQuery("" +
                        "select " +
                        " window_start stt, " +
                        " window_end edt, " +
                        " 'search' source" +
                        " kw keyword, " +
                        " date_format(  ) cur_date, " +  // 统计日期
                        " count(*) keyword_count " +
                        " from" +
                        " table( tumble( table kw_table, descriptor(et), interval '5' second ) ) " +  // flink sql 滚动窗口
                        " group by window_start, window_end, kw " +
                        "");// .execute()
// .print()

        // 5. 写入到doris

        /*
        10.1.4 Doris建表语句
        drop table if exists dws_traffic_source_keyword_page_view_window;
        create table if not exists dws_traffic_source_keyword_page_view_window
        (
            `stt`           DATETIME comment '窗口起始时间',
            `edt`           DATETIME comment '窗口结束时间',
            `source`        VARCHAR(10) comment '关键词来源',
            `keyword`       VARCHAR(10) comment '关键词',
            `cur_date`      DATE comment '当天日期',
            `keyword_count` BIGINT replace comment '关键词评分'
        ) engine = olap aggregate key (`stt`, `edt`, `source`, `keyword`, `cur_date`)
        comment "流量域来源-关键词粒度页面浏览汇总表"
        partition by range(`cur_date`)()
        distributed by hash(`keyword`) buckets 10
        properties (
          "replication_num" = "3",
          "dynamic_partition.enable" = "true",
          "dynamic_partition.time_unit" = "DAY",
          "dynamic_partition.start" = "-1",
          "dynamic_partition.end" = "1",
          "dynamic_partition.prefix" = "par",
          "dynamic_partition.buckets" = "10",
          "dynamic_partition.hot_partition_num" = "1"
        );
        * */
        // todo 这里为啥还要建立doris的表，上面不是通过命令行创建了表吗？
        tEnv.executeSql("create table kw(" +
                " stt string, " +
                " edt string, " +
                " source string," +
                " keyword string, " +
                " cur_date string, " +
                " keyword_count bigint " +
                ") with(" +
                " 'connector'='doris', " +
                " 'fenodes'='doris', " +
                " 'table.identifier'='gmall2022.dws_traffic_source_keyword_page_view_window', " +
                " 'username'='root', " +
                " 'password'='aaaaaa', " +
                ")")
                ;

        //
        //
    }
}

/*
idea 输出乱码，如何解决？
+----+--------------------------------+-------------------------+
| op |                        keyword |                      et |
+----+--------------------------------+-------------------------+
| +I |                             ?? | 2024-09-11 20:24:12.000 |
| +I |                          apple | 2024-09-11 20:24:24.000 |
| +I |                          apple | 2024-09-11 20:24:24.000 |
| +I |                           ???? | 2024-09-11 20:24:34.000 |
| +I |                           ???? | 2024-09-11 20:24:34.000 |
 */