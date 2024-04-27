package com.atguigu.hbase.util;

import com.sun.tools.javac.util.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jetty.util.StringUtil;

import java.io.IOException;

/*
* 读写数据库，把读写的常用方法编写工具类，在业务代码中调用
*
* 工具类编程敏感性：客户端---->链接---->发命令---->服务端
*
* 如何实例化链接：ConnectFactory 连接工厂
* 谁调用，谁管理。不用链接，及时关闭，释放资源。
* 从Connection获取Table（操作HBase表，DML）和Admin（管理表，DDL）对象
* Connection是重量级，不建议频繁创建，且是线程安全的，可以在不同的线程中共享，一般是一个APP创建一次
* Table和Admin是轻量级的，线程不安全，每个线程都有自己的Table和Admin，不建议池化和缓存
*
* 线程安全：对象可以声明  静态变量static 或 成员变量（类体中）
* 线程不安全：对象声明为方法内部的局部变量
*
* -------------------------------------
* 客户端怎么连上服务端？
*      通过zk
*      客户端不管是读写，都要找zk
* -------------------------------------
* 增、改 ： Table.put(Put put)
* 删除： Table.delete(Delete d)
* 查单行：Table.get(Get g)
* 查多行：Table.scan(Scanner s)
*
* -------------------------------------
* 工具类：
*   Bytes:
*       将常见的数据类型转byte[]：  Bytes.toBytes(xxx)
*       将byte[]转常见的数据类型: Bytes.toXxx(byte[] b)
*   CellUtil:
*       CellUtil.cloneXxx(Cell c)：将cell的某个xxx(rowkey,column family, colun qualfiter, value)属性获取到
*
* */
public class HBaseUtil {

    // 成员变量
    Connection connection = null;
    // 类被加载的时候，直接生成connection
    {
        try {
            // return createConnection(HBaseConfiguration.create(), null, null);
            // 只要是连接HBase，在创建Configuration时候，只能用HBaseConfiguration.create()创建
            // 不能直接new Configuration()
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 关闭connection
    public void close(Connection connection) throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    /*
    * 根据表名获取可以操作表的Table对象
    * */
    public Table getTable(String tableName) throws IOException {
        // 对表名进行校验
        // isBlank(xx) 当xx是null或 "" 或 白字符（空格，回车，\t）返回true
        if (StringUtil.isBlank(tableName)){
            System.err.println("表名非法!");
            throw new RuntimeException("表名非法!");
        }
        return connection.getTable(TableName.valueOf(tableName));
    }

    /*
    * 封装Put对象的方法
    * */
    public Put getPut(String rowkey, String cf, String cq, String value){
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cq),Bytes.toBytes(value));
        return put;
    }

    /*
    * 遍历单行查询的结果
    * */
    public void parseResult(Result result){
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("rowkey: " + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("cf:cq: " + Bytes.toString(CellUtil.cloneFamily(cell)) +":"+ Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("value: " + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

}
