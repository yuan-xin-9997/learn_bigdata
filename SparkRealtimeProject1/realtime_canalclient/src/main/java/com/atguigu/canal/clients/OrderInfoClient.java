package com.atguigu.canal.clients;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import java.net.InetSocketAddress;

/**
 * @author: yuan.xin
 * @createTime: 2024/05/14 21:51
 * @contact: yuanxin9997@qq.com
 * @description:
 *
 * ①先创建一个客户端对象CanalConnector
 *
 * ②使用客户端对象连接 Canal server端
 *
 * ③订阅表
 *
 * ④解析订阅到的数据
 *
 * ⑤将数据写入kafka
 */
public class OrderInfoClient {
    public static void main(String[] Args) {
        // ①先创建一个客户端对象CanalConnector
        // public static CanalConnector newSingleConnector(SocketAddress address, String destination, String username, String password)
        // SocketAddress address
        //      hostname  即canal.properties的canal.ip
        //      port  即canal.properties的canal.port
        // String destination
        //        即canal.properties的canal.destinations   订阅的mysql实例配置文件instance.properties所在目录名
        // String username 没有 注意此处不要和properties文件里面canal服务端连mysql数据库的密码和账号混淆了
        // String password 没有 注意此处不要和properties文件里面canal服务端连mysql数据库的密码和账号混淆了
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop103", 11111),
                "example", null, null);

    }
}
