package com.atguigu.hbase.util;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class HBaseUtilTest {

    private HBaseUtil hBaseUtil = new HBaseUtil();
    String tableName = "student";
    @Test
    public void testGetTable() throws IOException {
        Table t1 = hBaseUtil.getTable(tableName);

        // 封装一个Get对象
//        Get get = new Get(Bytes.toBytes("1001"));
        Get get = new Get(Bytes.toBytes("1005"));
        Result result = t1.get(get);

        hBaseUtil.parseResult(result);
        t1.close();
    }

    @Test
    public void testPut() throws IOException {
        Table t1 = hBaseUtil.getTable(tableName);

        // 封装一个Put对象
        Put put1 = hBaseUtil.getPut("1005", "info", "name", "jack");
        Put put2 = hBaseUtil.getPut("1005", "info", "age", "50");
        Put put3 = hBaseUtil.getPut("1005", "info", "gender", "male");

        ArrayList<Put> puts = new ArrayList<>();
        puts.add(put1);
        puts.add(put2);
        puts.add(put3);

        t1.put(puts);

        t1.close();
    }

    @Test
    public void testScan() throws IOException {
        Table t1 = hBaseUtil.getTable(tableName);

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("1001"));
        scan.withStopRow(Bytes.toBytes("1005"));

        // Result(1行)的集合
        ResultScanner scanner = t1.getScanner(scan);
        for (Result result : scanner) {
            hBaseUtil.parseResult(result);
        }

        t1.close();
    }

    @Test
    public void testDelete() throws IOException {
        Table t1 = hBaseUtil.getTable(tableName);

        // 构造删除一行的一个Delete对象
        Delete delete = new Delete(Bytes.toBytes("1001"));

        // 进一步明确删除那一列
        // 增加一个cell，timestamp=当前劣最新的cell的时间戳，type=Delete。删除这列的最新版本
//        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
//                        .addColumn()

        // 增加一个cell，timestamp=当前劣最新的时间戳，type=DeleteColumn。删除整个列
//        delete.addColumns(Bytes.toBytes("info"), Bytes.toBytes("age"));

        // 增加一个cell，column=列族,timestamp=当前列最新的时间戳，type=DeleteFamily。删除整个列族
        delete.addFamily(Bytes.toBytes("info"));

        // 如果没有明确指定删除那一列，则相当于对每个列族，都执行delete.addFamily()
        t1.delete(delete);

        t1.close();
    }
}