package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    public void upload() throws IOException {
         System.out.println("--------upload---------");
         /*
          * copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
          *    delSrc 是否删除原文件（本地的）
          *    overwrite 是否覆盖
          *    src 源文件路径（本地）
          *    dst 目标文件路径
          */
        fs.copyFromLocalFile(false, true, new Path("C:\\Windows\\regedit.exe"), new Path("/input"));
    }

    // 下载
    @Test
    public void download() throws IOException {
         System.out.println("--------download---------");
         /*
         * public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException
         *      delSrc: 是否删除源文件（HDFS）
         *      src：源文件路径（HDFS）
         *      dst：目标文件路径（本地）
         *      useRawLocalFileSystem：是否使用本地文件系统
         * */
         fs.copyToLocalFile(false, new Path("/input/xsync.sh"), new Path("D:\\dev\\"), true);
    }
}
