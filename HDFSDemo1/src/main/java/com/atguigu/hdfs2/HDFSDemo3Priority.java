package com.atguigu.hdfs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
* 参数设置的优先级：
*   1. 在IDEA中的操作HDFS
*   参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的默认配置
*
*   2. 在服务器通过命令操作HDFS
*       服务器端的配置文件(xxx-site.xml)>服务器端默认配置(xxx-default.xml)
* */

public class HDFSDemo3Priority {


    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        // 1. 创建客户端对象
        /*
        * get(final URI uri, final Configuration conf, String user)
        * */
        Configuration conf = new Configuration();
        URI url = new URI("hdfs://hadoop102:8020");
        conf.set("dfs.replication", "2");  // 设置副本数
        FileSystem fs = FileSystem.get(url, conf, "atguigu");

        // 2. 上传文件
        fs.copyFromLocalFile(false, true, new Path("C:\\Windows\\regedit.exe"), new Path("/input"));

         // 3.关闭资源
        fs.close();
    }
}
