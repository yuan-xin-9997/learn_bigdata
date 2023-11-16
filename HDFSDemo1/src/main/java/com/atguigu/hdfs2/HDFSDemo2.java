package com.atguigu.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/*
* 错误信息：Permission denied: user=yuanx, access=WRITE, inode="/input":atguigu:supergroup:drwxr-xr-x
*
*
* 解决方案：
*   1. 设置权限
*   2. 设置操作HDFS文件系统的用户，IDEA VM options添加“-DHADOOP_USER_NAME=atguigu”
* */

public class HDFSDemo2 {
    public static void main(String[] args) throws IOException {
        // 创建客户端对象的 第二种方式
        // 1.创建客户端对象
        Configuration conf = new Configuration();
        // 设置参数，NameNode的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        FileSystem fs = FileSystem.get(conf);

        // 2. 上传文件
        fs.copyFromLocalFile(false, true, new Path("C:\\Windows\\regedit.exe"), new Path("/input"));

         // 3.关闭资源
        fs.close();
    }
}
