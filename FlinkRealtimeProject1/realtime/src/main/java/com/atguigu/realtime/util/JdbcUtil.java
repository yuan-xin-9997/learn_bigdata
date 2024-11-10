package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.Constant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 获取数据库连接工具类
 * @author: yuan.xin
 * @createTime: 2024/08/05 21:14
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class JdbcUtil {

    /**
     * 获取JDBC连接
     * @param driver
     * @param url
     * @param username
     * @param password
     * @return
     */
    public static Connection getJdbcConnection(String driver, String url, String username, String password) {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("你提供的驱动类未找到，请检查数据库连接器依赖是否导入，或驱动名字是否正确" + driver);
        }
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("你提供的URL或用户名/密码 有错误: url=" + url + ", user=" + username + ",password=" + password);
        }
    }

    /**
     * 获取Phenix连接
     * @return
     */
    public static Connection getPhenixConnection() {
        String driver = Constant.PHENIX_DRIVER;
        String url = Constant.PHENIX_URL;
        return getJdbcConnection(driver, url, null, null);
    }

    /**
     * 关闭数据库连接
     * @param conn
     */
    public static void closeConnection(Connection conn) {
        try {
            if (conn != null &&  !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 查询数据
     * @param conn
     * @param querySql
     * @param args
     * @return
     */
    public static <T> List<T> queryList(Connection conn, String querySql, Object[] args, Class<T> tClass) {
        ArrayList<T> result = new ArrayList<>();
        try {
            PreparedStatement ps = conn.prepareStatement(querySql);
            // 给SQL中的占位符赋值
            for (int i = 0; args != null && i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            ResultSet resultSet = ps.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();  // 元数据：列名 列的类型 列的别名
            int columnCount = metaData.getColumnCount();  // 查询结果有多少列
            // 遍历出结果集中的每一行
            while (resultSet.next()) {
                // 将每一行数据转换为T对象，然后放入到result集合中
                T t = tClass.newInstance();  // 利用反射创建一个T类型的对象
                // 给T中属性赋值，属性的值从resultSet获取
                // 从resultSet里面查看有多少列，每一列在T中应该对应一个属性，名字应该完全一样
                for (int i = 1; i <= columnCount; i++) {
                    // 获取列名
                    String columnLabel = metaData.getColumnLabel(i);
                    Object v = resultSet.getObject(i);
                    BeanUtils.setProperty(t, columnLabel, v);
                }
                result.add(t);
            }
        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * 内部类
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class TM{
        private String tm_name;
        private String logo_url;
        private int id;
    }

    /**
     * 测试
     * @param Args
     */
    public static void main(String[] Args) {
        // List<JSONObject> list = queryList(
        //         getPhenixConnection(),
        //         // "select * from dim_sku_info where id=?",
        //         "select * from dim_sku_info",
        //         null,
        //         JSONObject.class);
        // for (JSONObject obj : list) {
        //     System.out.println(obj);
        // }

        List<TM> list = queryList(
                getJdbcConnection("com.mysql.jdbc.Driver", "jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false", "root", "aaaaaa"),
                "select * from base_trademark",
                // "select * from spu_info",
                null,
                TM.class
        );
        for (TM obj : list) {
            System.out.println(obj);
        }
    }



}
