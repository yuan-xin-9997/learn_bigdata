package com.atguigu.realtime.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/10/31 19:57
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class DimUtil {
    public static void main(String[] Args) {

    }

    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String table, String id) {
        String sql = "select * from " + table + " where id = ?";
        String[] args = {id};
        // 一个通用的JDBC查询方法，返回值可能有多行，所以返回list集合
        List<JSONObject> list = JdbcUtil.queryList(phoenixConn, sql, args, JSONObject.class);
        return list.get(0);  // 对当前SQL语句，一定只有一行，直接get(0)
    }
}
