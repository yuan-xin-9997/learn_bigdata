package com.atguigu.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.DruidDSUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/06 21:22
 * @contact: yuanxin9997@qq.com
 * @description:  自定义 Flink PhoenixSink
 *
 * 待解决连接池被关闭的问题
 * todo: 201055 [Co-Process-Broadcast -> Map -> Sink: Unnamed (1/1)#6] WARN  org.apache.flink.runtime.taskmanager.Task
 *  Co-Process-Broadcast -> Map -> Sink: Unnamed (1/1)#6 (5685fe4749b4f6687de1044e98f61a60) switched
 *  from RUNNING to FAILED with failure cause:
 *  com.alibaba.druid.pool.DataSourceClosedException: dataSource already closed at Wed Aug 07 20:41:46 GMT+08:00 2024
 * 	at com.alibaba.druid.pool.DruidDataSource.getConnectionInternal(DruidDataSource.java:1447)
 * 	at com.alibaba.druid.pool.DruidDataSource.getConnectionDirect(DruidDataSource.java:1337)
 * 	at com.alibaba.druid.pool.DruidDataSource.getConnection(DruidDataSource.java:1317)
 * 	at com.alibaba.druid.pool.DruidDataSource.getConnection(DruidDataSource.java:1307)
 * 	at com.atguigu.realtime.sink.PhoenixSink.invoke(PhoenixSink.java:36)
 * 	at com.atguigu.realtime.sink.PhoenixSink.invoke(PhoenixSink.java:23)
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
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private DruidDataSource dataSource;  // 连接池对象

    /**
     * 写方法
     * @param value The input record.
     * @param context Additional context about the input record.
     * @throws Exception
     */
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        // 每来一条数据，从连接池获取一个可用的连接，可以避免长链接被服务器自动关闭的问题
        if(dataSource.isClosed()){
            dataSource = DruidDSUtil.getDruidDataSource();
        }
        DruidPooledConnection conn = dataSource.getConnection();

        JSONObject data = value.f0;
        TableProcess tp = value.f1;

        // 1. 拼接SQL语句，一定有占位符
        // upsert into user(a,b,c) values(?,?,?)
        StringBuilder sql = new StringBuilder();
        sql
                .append("upsert into ")
                .append(tp.getSinkTable())
                .append(" (")
                .append(tp.getSinkColumns())
                .append(" )values(")
                .append(tp.getSinkColumns().replaceAll("[^,]+", "?"))
                .append(" )")
                ;
        System.out.println("upsert SQL 语句: " + sql);
        // 2. 使用连接对象获取一个PrepareStatement
        PreparedStatement ps = conn.prepareStatement(sql.toString());

        // 3. 给占位符赋值
        String[] cs = tp.getSinkColumns().split(",");
        for (int i = 0, count = cs.length; i < count; i++) {
            Object v = data.get(cs[i]);
            String vv = v == null ? null : v.toString();
            ps.setString(i + 1, vv);
        }
//        for (String columnName : tp.getSinkColumns().split(",")) {
//        }

        // 4. 执行SQL
        ps.execute();
        conn.commit();  // 提交

        // 关闭PrepareStatement
        ps.close();

        // 5. 归还连接给连接池
        conn.close();
    }

    /**
     * 建立数据库连接池
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // conn = JdbcUtil.getPhenixConnection();
        // 获取连接池
//        if (!dataSource.isClosed()) {
            dataSource = DruidDSUtil.getDruidDataSource();
//        }
    }

    /**
     * 关闭数据库连接池
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if (!dataSource.isClosed()) {
            dataSource.close();
        }
    }
}
