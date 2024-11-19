package com.atguigu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import com.atguigu.realtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;


/**
 * @author: yuan.xin
 * @createTime: 2024/07/31 20:05
 * @contact: yuanxin9997@qq.com
 * @description: flink实时数仓开发-ods层(Kafka) 到 dim层(HBase + Phoenix)
 *
 *
 * -------------------------------------------
 * 数仓分层规划：
 * 数仓分层
 *
 * ods
 *
 * 原始数据层
 * 存储位置: kafka
 *
 * topic:
 *     日志
 *         数据采集的数据到了kakfa, 其实就是ods层
 *
 *             ods_log
 *
 *
 *     业务数据
 *         maxwell
 *
 *             ods_db
 *
 * dim
 *    维度
 *
 *    存储位置:
 *       存入到支持sql的数据库中
 *         查询的频率比较高, 数据量还比较大
 *
 *         mysql: 可以选, 不是很好
 *
 *         hbase+Phoenix: 可以接入在线业务. 使用这个
 *             公司不是很方便
 *
 *         es:  支持 sql
 *             dsl 也可以直接读
 *
 *         redis: 主要在容器
 *
 *         hive: 不行
 *
 *    维度的用法:
 *
 *       假设有订单数据
 *         user_id   province_id
 *
 *         根据各种id查找对应的维度信息
 *
 *         user_1     province_1
 *
 *         随机查找
 *             select ...  from dim where id=?
 *
 *
 *
 *
 *
 *
 *
 * dwd
 * 来源于ods
 * 数据明细层
 *     日志: 启动 页面日志 曝光 活动 错误
 *
 *           分流:
 *                 每种日志写入到不同的topic中\
 *
 *                 用的流的方式
 *                     侧输出流
 *
 *     业务: 事实表 维度
 *       只管理事实表
 *
 *         对感兴趣的业务进行处理
 *             会对一些特别频繁使用的维度做退化,退化到事实表
 *
 *            使用sql技术
 *
 *            join ...
 *
 *
 * 存储位置?
 *     kafka, 给dws使用
 * dws
 * 数据汇总层
 *     轻度汇总
 *         小窗口(预聚合)
 *
 *     0-10
 *     10-20
 *
 *     存储到哪里?
 *         支持sql的数据
 *
 *         hbase+Phoenix 不选
 *
 *         clickhouse olap
 *
 *         drois olap
 *
 *
 *
 * ads
 *     最终的汇总
 *         非常的灵活
 *
 *     不做落盘, 根据前端的需求, 实时的汇总
 *
 *     select .. from t where time >=.. and time <= ... group by ...
 *
 *
 * -----------------------------
 * HBase
 *  SALT_BUCKETS = 4 盐表
 *
 *  regionserver
 *  region  存数据
 *
 *  默认情况下，建一张表 只有一个region，当region膨胀到一定程度后，会自动分裂，分裂算法（旧版本：到10G，一分为2，新版本：）
 *  Region Split
 *  默认情况下，每个Table起初只有一个Region，随着数据的不断写入，Region会自动进行拆分。刚拆分时，两个子Region都位于当前的Region
 *  Server，但处于负载均衡的考虑，HMaster有可能会将某个Region转移给其他的Region Server。
 *
 *  hadoop162 r1 r2
 *  自动迁移到其他节点：r2 迁移到163  在凌晨或半夜
 *
 *  生产环境下，为了避免分裂和迁移：进行预分区
 *  --------------------------
 *  Phoenix 如何建立预分区的表
 *
 */
public class Ods2Dim extends BaseAppV1 {

    public static void main(String[] Args) {
        new Ods2Dim().init(2001, 1, "Ods2Dim", Constant.TOPIC_ODS_DB);
    }

    /**
     * 重写业务处理函数
     *
     * @param env
     * @param stream
     */
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 对流的操作
        // 1. 对业务数据做ETL，获得业务数据流
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        //etledStream.print();

        // 2. 读取配置信息流（MySQL）
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        //tpStream.print();

        // 3. 数据流和广播流进行connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpStream = connect(etledStream, tpStream);
        //dataTpStream.print();

        // 4. 过滤掉JSONObject中不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterNotNeedColumnsStream = filterNotNeedColumns(dataTpStream);
        //filterNotNeedColumnsStream.print();

        // 5. 根据不同的配置信息，把不同的维度写入到不同的Phoenix表中
        writeToPhoenix(filterNotNeedColumnsStream);
    }

    /**
     * 将流数据写入到Phoenix
     * 可以通过SQL语句写，flink官方没有提供phoenix sink
     * JDBC连接器？由于jdbc一个流只能写入到一张表，不能使用jdbc sink   ref https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/jdbc/
     * 因此只能自定义 isnk
     * @param stream
     */
    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        // System.out.println("");
        stream
                .addSink(FlinkSinkUtil.getPhoenixSink())
                ;
    }

    /**
     * 过滤数据函数
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterNotNeedColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpStream) {
        return dataTpStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {
                        JSONObject data = value.f0;
                        List<String> columns = Arrays.asList(value.f1.getSinkColumns().split(","));
                        // data 是一个map集合，需要从data中删除键值对
                        data.keySet().removeIf(key->!columns.contains(key) && !"op_type".equals(key));
                        return value;
                    }
                })
//                .print()
                ;
    }

    /**
     * 连接数据流和配置流
     *
     * @param dataStream
     * @param tpStream
     * @return
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<JSONObject> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        // 0.根据配置信息，在Phoenix中创建相应的维度表
        tpStream = tpStream
                .map(new RichMapFunction<TableProcess, TableProcess>() {
                    private Connection conn;
                    /**
                     * 创建数据库连接
                     * @param parameters The configuration containing the parameters attached to the contract.
                     * @throws Exception
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = JdbcUtil.getPhenixConnection();
                    }

                    /**
                     * 关闭数据库连接
                     * @throws Exception
                     */
                    @Override
                    public void close() throws Exception {
                        JdbcUtil.closeConnection(conn);
                    }

                    /**
                     * 执行建表语句
                     * @param tp The input value.
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public TableProcess map(TableProcess tp) throws Exception {
                        // 判断连接是否被数据库服务端关闭（比如长链接超时长时间没有使用，服务器会自动关闭连接）或者使用连接池
                        if (conn.isClosed()) {
                            conn = JdbcUtil.getPhenixConnection();
                        }

                        // 建表操作
                        // create table if not exists table_name(name varchar, age varchar, constraint pk primary key(name))
                        // JDBC 操作 Phoenix，一个并行度一个连接

                        // 1. 拼接一个SQL语句
                        StringBuilder sql = new StringBuilder();
                        sql
                                .append("create table if not exists ")
                                .append(tp.getSinkTable())
                                .append("(")
                                .append(tp.getSinkColumns().replaceAll("[^,]+", "$0 varchar"))
                                .append(", constraint pk primary key ( ")
                                .append(tp.getSinkPk() == null ? "id" : tp.getSinkPk())  // MySQL配置表中SinkPk字段可能为空
                                .append("))")
                                .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend())
                        ;

                        System.out.println("phenix建表语句: " + sql);

                        // 2. 获取预处理语句
                        PreparedStatement ps = conn.prepareStatement(sql.toString());

                        // 3. 给SQL中占位符赋值(增删改查）,ddl建表语句一般不会有占位符
                        // 略

                        // 4. 执行
                        ps.execute();

                        // 5. 关闭ps
                        if (ps != null) {
                            ps.close();
                        }
                        return tp;
                    }
                });

        // 1. 把配置流做成广播流
        // key: source_table
        // value: TableProcess
        MapStateDescriptor<String, TableProcess> tpStateDescriptor = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBcStream = tpStream.broadcast(tpStateDescriptor);

        // 2. 让数据流去connect广播流
        return dataStream
                .connect(tpBcStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    /**
                     * 处理数据流中的元素
                     * @param value The stream element.
                     * @param ctx A {@link ReadOnlyContext} that allows querying the timestamp of the element,
                     *     querying the current processing/event time and updating the broadcast state. The context
                     *     is only valid during the invocation of this method, do not store it.
                     * @param out The collector to emit resulting elements to
                     * @throws Exception
                     */
                    @Override
                    public void processElement(JSONObject value,  // 从kafka读取的数据
                                               BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 4. 处理数据流中数据的时候，从广播状态读取他对应的配置信息
                        // 根据什么获取配置信息：根据MySQL中的表名
                        ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDescriptor);  // 获取广播状态
                        String key = value.getString("table");  // kafka中数据的表名
                        TableProcess tp = state.get(key);
                        // ？如果不是维度表，或者并不需要sink的维度表，tp应该是null
                        if (tp != null) {
                            // 数据中的元数据信息可以不用，可以只取数据信息
                            JSONObject data = value.getJSONObject("data");
                            data.put("op_type", value.getString("type"));  // 把操作类型，写入到data中，后续有用
                            out.collect(Tuple2.of(data, tp));
                        }
                    }

                    /**
                     * 处理广播流中的元素
                     * @param value The stream element.
                     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
                     *     current processing/event time and updating the broadcast state. The context is only valid
                     *     during the invocation of this method, do not store it.
                     * @param out The collector to emit resulting elements to
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 3. 把配置信息写入到广播状态
                        String key = value.getSourceTable();
                        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(tpStateDescriptor);
                        broadcastState.put(key, value);
                    }
                });
    }

    /**
     * 使用Flink MySQL CDC 读取配置表信息（读取全量+实时变化的数据）
     * @param env
     */
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop162")  // MySQL主机名
                .port(3306)
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process") // set captured table
                .username("root")
                .password("aaaaaa")
                //.debeziumProperties()
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        SingleOutputStreamOperator<TableProcess> stream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                //.print();
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    return obj.getObject("after", TableProcess.class);
                });
        return stream;
    }

    /**
     * 过滤数据
     *
     * 注意：maxwell可使用bootstrap一次性全量读取mysql数据，并且以增量方式写入到kafka：
     *    bin/maxwell-bootstrap --config config.properties --database gmall2022 --table spu_info
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
         return stream
                .filter(json -> {
                    try {
                        // JSONObject obj = JSONObject.parseObject(json);
                        JSONObject obj = JSONObject.parseObject(json.replaceAll("bootstrap-", ""));
                        return "gmall2022".equals(obj.getString("database")) && (
                                "insert".equals(obj.getString("type")) ||
                                "update".equals(obj.getString("type"))
                                ) && obj.getString("data") != null
                                && obj.getString("data").length() > 2;
                    } catch (Exception e) {
                        System.out.println("json格式有误：" + json);
                        return false;
                    }
                })
                 //.map(json -> JSONObject.parseObject(json));  // 转成JSON Object 方便后续使用
                 .map(JSONObject::parseObject);  // 转成JSON Object 方便后续使用

    }
}


/**
 * FLink on Yarn - Yarn Session 模式运行报错：内存溢出
 * 2024-11-19 20:22:57
 * java.sql.SQLException: java.lang.OutOfMemoryError: unable to create new native thread
 * 	at org.apache.phoenix.query.ConnectionQueryServicesImpl.metaDataCoprocessorExec(ConnectionQueryServicesImpl.java:1390)
 * 	at org.apache.phoenix.query.ConnectionQueryServicesImpl.metaDataCoprocessorExec(ConnectionQueryServicesImpl.java:1351)
 * 	at org.apache.phoenix.query.ConnectionQueryServicesImpl.getTable(ConnectionQueryServicesImpl.java:1570)
 * 	at org.apache.phoenix.schema.MetaDataClient.updateCache(MetaDataClient.java:644)
 * 	at org.apache.phoenix.schema.MetaDataClient.updateCache(MetaDataClient.java:538)
 * 	at org.apache.phoenix.schema.MetaDataClient.updateCache(MetaDataClient.java:530)
 * 	at org.apache.phoenix.schema.MetaDataClient.updateCache(MetaDataClient.java:526)
 * 	at org.apache.phoenix.execute.MutationState.validateAndGetServerTimestamp(MutationState.java:752)
 * 	at org.apache.phoenix.execute.MutationState.validateAll(MutationState.java:740)
 * 	at org.apache.phoenix.execute.MutationState.send(MutationState.java:870)
 * 	at org.apache.phoenix.execute.MutationState.send(MutationState.java:1344)
 * 	at org.apache.phoenix.execute.MutationState.commit(MutationState.java:1167)
 * 	at org.apache.phoenix.jdbc.PhoenixConnection$3.call(PhoenixConnection.java:670)
 * 	at org.apache.phoenix.jdbc.PhoenixConnection$3.call(PhoenixConnection.java:666)
 * 	at org.apache.phoenix.call.CallRunner.run(CallRunner.java:53)
 * 	at org.apache.phoenix.jdbc.PhoenixConnection.commit(PhoenixConnection.java:666)
 * 	at org.apache.phoenix.jdbc.PhoenixStatement$2.call(PhoenixStatement.java:411)
 * 	at org.apache.phoenix.jdbc.PhoenixStatement$2.call(PhoenixStatement.java:391)
 * 	at org.apache.phoenix.call.CallRunner.run(CallRunner.java:53)
 * 	at org.apache.phoenix.jdbc.PhoenixStatement.executeMutation(PhoenixStatement.java:390)
 * 	at org.apache.phoenix.jdbc.PhoenixStatement.executeMutation(PhoenixStatement.java:378)
 * 	at org.apache.phoenix.jdbc.PhoenixPreparedStatement.execute(PhoenixPreparedStatement.java:173)
 * 	at org.apache.phoenix.jdbc.PhoenixPreparedStatement.execute(PhoenixPreparedStatement.java:183)
 * 	at com.alibaba.druid.pool.DruidPooledPreparedStatement.execute(DruidPooledPreparedStatement.java:497)
 * 	at com.atguigu.realtime.sink.PhoenixSink.invoke(PhoenixSink.java:111)
 * 	at com.atguigu.realtime.sink.PhoenixSink.invoke(PhoenixSink.java:63)
 * 	at org.apache.flink.streaming.api.operators.StreamSink.processElement(StreamSink.java:54)
 * 	at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.pushToOperator(CopyingChainingOutput.java:71)
 * 	at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.collect(CopyingChainingOutput.java:46)
 * 	at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.collect(CopyingChainingOutput.java:26)
 * 	at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:50)
 * 	at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:28)
 * 	at org.apache.flink.streaming.api.operators.StreamMap.processElement(StreamMap.java:38)
 * 	at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.pushToOperator(CopyingChainingOutput.java:71)
 * 	at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.collect(CopyingChainingOutput.java:46)
 * 	at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.collect(CopyingChainingOutput.java:26)
 * 	at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:50)
 * 	at org.apache.flink.streaming.api.operators.CountingOutput.collect(CountingOutput.java:28)
 * 	at org.apache.flink.streaming.api.operators.TimestampedCollector.collect(TimestampedCollector.java:50)
 * 	at com.atguigu.realtime.app.dim.Ods2Dim$3.processElement(Ods2Dim.java:351)
 * 	at com.atguigu.realtime.app.dim.Ods2Dim$3.processElement(Ods2Dim.java:327)
 * 	at org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator.processElement1(CoBroadcastWithNonKeyedOperator.java:110)
 * 	at org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory.processRecord1(StreamTwoInputProcessorFactory.java:213)
 * 	at org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory.lambda$create$0(StreamTwoInputProcessorFactory.java:178)
 * 	at org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory$StreamTaskNetworkOutput.emitRecord(StreamTwoInputProcessorFactory.java:291)
 * 	at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.processElement(AbstractStreamTaskNetworkInput.java:134)
 * 	at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.emitNext(AbstractStreamTaskNetworkInput.java:105)
 * 	at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:66)
 * 	at org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor.processInput(StreamTwoInputProcessor.java:96)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:423)
 * 	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:204)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:684)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.executeInvoke(StreamTask.java:639)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.runWithCleanUpOnFail(StreamTask.java:650)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:623)
 * 	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:779)
 * 	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:566)
 * 	at java.lang.Thread.run(Thread.java:748)
 * Caused by: java.lang.OutOfMemoryError: unable to create new native thread
 * 	at java.lang.Thread.start0(Native Method)
 * 	at java.lang.Thread.start(Thread.java:717)
 * 	at java.util.concurrent.ThreadPoolExecutor.addWorker(ThreadPoolExecutor.java:957)
 * 	at java.util.concurrent.ThreadPoolExecutor.execute(ThreadPoolExecutor.java:1367)
 * 	at java.util.concurrent.AbstractExecutorService.submit(AbstractExecutorService.java:134)
 * 	at org.apache.hadoop.hbase.client.HTable.coprocessorService(HTable.java:1007)
 * 	at org.apache.hadoop.hbase.client.HTable.coprocessorService(HTable.java:986)
 * 	at org.apache.phoenix.query.ConnectionQueryServicesImpl.metaDataCoprocessorExec(ConnectionQueryServicesImpl.java:1372)
 * 	... 57 more
 */