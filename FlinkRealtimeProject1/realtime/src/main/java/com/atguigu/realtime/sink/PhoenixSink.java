package com.atguigu.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.DruidDSUtil;
import com.atguigu.realtime.util.JdbcUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/06 21:22
 * @contact: yuanxin9997@qq.com
 * @description:  自定义 Flink PhoenixSink
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
        dataSource = DruidDSUtil.getDataSource();
    }

    /**
     * 关闭数据库连接池
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        dataSource.close();
    }
}
