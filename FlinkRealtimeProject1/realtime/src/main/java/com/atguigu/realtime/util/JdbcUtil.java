package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 获取数据库连接工具类
 * @author: yuan.xin
 * @createTime: 2024/08/05 21:14
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class JdbcUtil {
    public static void main(String[] Args) {

    }

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
}
