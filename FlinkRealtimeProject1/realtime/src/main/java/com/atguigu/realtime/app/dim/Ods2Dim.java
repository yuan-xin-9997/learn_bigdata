package com.atguigu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSourceUtil;
import com.atguigu.realtime.util.JdbcUtil;
import com.mysql.cj.xdevapi.Table;
import com.sun.org.apache.xpath.internal.operations.String;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static java.lang.System.setProperty;


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
 */
public class Ods2Dim extends BaseAppV1 {

    public static void main(String[] Args) {
        new Ods2Dim().init(2001, 2, "Ods2Dim", Constant.TOPIC_ODS_DB);
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
        tpStream.print();

        // 3. 数据流和广播流进行connect
        connect(etledStream, tpStream);

        // 4. 根据不同的配置信息，把不同的维度写入到不同的Phoenix表中

    }

    /**
     * 连接数据流和配置流
     * @param dataStream
     * @param tpStream
     */
    private void connect(SingleOutputStreamOperator<JSONObject> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
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
                        // 建表操作
                        // create table if not exists table_name(name varchar, age varchar, constraint pk primary key(name))
                        // JDBC 操作 Phoenix，一个并行度一个连接

                        // 1. 拼接一个SQL语句
                        StringBuilder sql = new StringBuilder();
                        sql
                                .append("crate table if not exists ")
                                .append(tp.getSinkPk())
                                .append("(")
                                .append(tp.getSinkColumns().replaceAll("[^,]+", "$0 varchar"))
                                .append(", constraint pk primary key ( ")
                                .append(tp.getSinkPk())  // MySQL配置表中SinkPk字段可能为空
                                .append("))")
                        ;

                        System.out.println("phenix建表语句: " + sql);

                        // 2. 获取预处理语句
                        PreparedStatement ps = conn.prepareStatement(sql);

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
        dataStream
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
                    public void processElement(JSONObject value,
                                               BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 4. 处理数据流中数据的时候，从广播状态读取他对应的配置信息
                        // 根据什么获取配置信息：根据MySQL中的表名
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

                        // 在Phoenix+HBase中建表（最好放到广播之前，否则会出现多次建表的情况，导致效率低下） 由于并行度大于1，此处会建表次数=并行度数量，广播传播到每个并行度中
                        //checkTable(value);
                    }

                    /**
                     * 根据读取到配置信息，在HBase+Phoenix中建表
                     * @param value
                     */
//                    private void checkTable(TableProcess value) {
//                        System.out.println(value.getSourceTable() + " " + value);
//
//                    }
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
