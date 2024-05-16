package com.atguigu.canal.clients;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.realtime.constants.TopicConstant;
import com.atguigu.realtime.utils.KafkaProducerUtil;
import com.atguigu.realtime.utils.PropertiesUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

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
    public static void main(String[] Args) throws InterruptedException, InvalidProtocolBufferException {
        // ①先创建一个客户端对象CanalConnector
        // public static CanalConnector newSingleConnector(SocketAddress address, String destination, String username, String password)
        // SocketAddress address
        //      hostname  即canal.properties的canal.ip
        //      port  即canal.properties的canal.port
        // String destination
        //        即canal.properties的canal.destinations   订阅的mysql实例配置文件instance.properties所在目录名
        // String username 没有 注意此处不要和properties文件里面canal服务端连mysql数据库的密码和账号混淆了
        // String password 没有 注意此处不要和properties文件里面canal服务端连mysql数据库的密码和账号混淆了
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(PropertiesUtil.getProperty("canal.host"), Integer.parseInt(PropertiesUtil.getProperty("canal.port"))),
                "example", null, null);

        // ②使用客户端对象连接 Canal server端
        canalConnector.connect();

        // ③订阅表   格式：库名.表名
        canalConnector.subscribe("spark_realtime.order_info");

        // 拉取数据，④解析订阅到的数据
        while (true) {
            Message message = canalConnector.get(100);
            if (message.getId() == -1) {
                // 没有拉取到数据
                System.out.println("当前没有新数据，休息5s");
                // 歇会
                Thread.sleep(5000);
                // 跳过本次循环，开始下次循环
                continue;
            }
            // 打印数据
            //System.out.println(message);
            List<CanalEntry.Entry> entries = message.getEntries();
            for (CanalEntry.Entry entry : entries) {
                // 是rowdata 才可能是insert/upaate/delete
                if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                    parseData(entry.getStoreValue());
                }
            }

        }

    }

    /**
     * 只要order_info表的insert语句的结果
     * 如果是update操作，会有变化前和变化后，    maxwell: {data:{变化后},old:{变化前}}
     * insert 操作，只有变化
     * @param storeValue
     * @throws InvalidProtocolBufferException
     */
    private static void parseData(ByteString storeValue) throws InvalidProtocolBufferException {
        // rowChange 反序列化后的1行sql导致的多行反序列化结果
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
        if (rowChange.getEventType().equals(CanalEntry.EventType.INSERT)) {
            // 多行数据变化
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            // rowData 1行数据变化
            for (CanalEntry.RowData rowData : rowDatasList) {
                // 构建一个JSONObject，把每一行封装为一个{}
                JSONObject jsonObject = new JSONObject();
                // 获取变化后的每一列
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(),column.getValue());
                }
                // System.out.println(jsonObject);
                // 将订单数据发送到Kafka
                KafkaProducerUtil.sendData(jsonObject.toString(), TopicConstant.ORDER_INFO);
            }
        }
    }
}
