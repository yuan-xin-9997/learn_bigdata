package com.atguigu.hafs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
* 通过代码去操作HDFS
* 1. 创建客户端对象
* 2. 具体操作（上传、下载）
* 3. 关闭资源
*
* */
public class HDFSDemo {

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException, URISyntaxException {
        System.out.println("--------before---------");
        // 1. 创建客户端对象
        /*
        * get(final URI uri, final Configuration conf, String user)
        * */
        Configuration conf = new Configuration();
        URI url = new URI("hdfs://hadoop102:8020");
        fs = FileSystem.get(url, conf, "atguigu");
    }

    @After
    public void after(){
         System.out.println("--------after---------");
         // 3. 关闭资源
         if (fs != null){
             try{
                 fs.close();
             }catch (IOException e){
                 e.printStackTrace();
             }
         }
    }


    // 上传
    @Test
    public void upload(){
         System.out.println("--------upload---------");
         /*
          * copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
          */
        fs.copyFromLocalFile();
    }

    // 下载
    @Test
    public void download(){
         System.out.println("--------download---------");
    }
}
