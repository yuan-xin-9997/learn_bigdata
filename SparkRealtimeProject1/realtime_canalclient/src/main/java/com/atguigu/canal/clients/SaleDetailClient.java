package com.atguigu.canal.clients;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.realtime.constants.PrefixConstant;
import com.atguigu.realtime.constants.TopicConstant;
import com.atguigu.realtime.utils.KafkaProducerUtil;
import com.atguigu.realtime.utils.PropertiesUtil;
import com.atguigu.realtime.utils.RedisUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import redis.clients.jedis.Jedis;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

/**
 * @author: yuan.xin
 * @createTime: 2024/05/14 21:51
 * @contact: yuanxin9997@qq.com
 * @description:
 *
 * 1. 订阅order_info和order_detail的insert操作。将信息发送到Kafka
 * 2. 订阅user_info的insert和update操作，将信息发送到Redis
 *        user_info在Redis怎么存？
 *              当前的需求是，根据userId查询用户信息
 *  		    最终设计：
 *  			key:  userinfo:userId
 * 			    value:  string
 *
 */
public class SaleDetailClient {

    // Redis 客户端
    private static Jedis jedis = RedisUtil.getJedis();

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
        canalConnector.subscribe("spark_realtime.*");

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
                String tableName = entry.getHeader().getTableName();

                // 是rowdata 才可能是insert/upaate/delete
                if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                    parseData(entry.getStoreValue(), tableName);
                }
            }

        }

    }

    /**
     * 解析数据
     * @param storeValue
     * @throws InvalidProtocolBufferException
     */
    private static void parseData(ByteString storeValue, String tableName) throws InvalidProtocolBufferException, InterruptedException {
        // rowChange 反序列化后的1行sql导致的多行反序列化结果
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
        if ("order_info".equals(tableName) && rowChange.getEventType().equals(CanalEntry.EventType.INSERT)) {
            sendData(rowChange, TopicConstant.ORDER_INFO, true);
        }else if ("order_detail".equals(tableName) && rowChange.getEventType().equals(CanalEntry.EventType.INSERT)) {
            sendData(rowChange, TopicConstant.ORDER_DETAIL, true);
        }else if ("user_info".equals(tableName) &&
                (rowChange.getEventType().equals(CanalEntry.EventType.INSERT) || rowChange.getEventType().equals(CanalEntry.EventType.UPDATE))) {
            sendData(rowChange, null, false);
        }

    }

    /**
     * 发送数据
     * @param rowChange
     * @param topic
     * @param ifSendToKafka
     */
    private static void sendData(CanalEntry.RowChange rowChange, String topic, boolean ifSendToKafka) throws InterruptedException {
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
            if(ifSendToKafka) {
                // 模拟网络延迟故障，导致发送到Kafka变慢
//                Random random = new Random();
//                Thread.sleep(random.nextInt(5) * 1000);
                // 将数据发送到Kafka
                KafkaProducerUtil.sendData(jsonObject.toString(), topic);
            }else {
                // 将数据写入Redis
                jedis.set(PrefixConstant.user_info_redis_preffix + jsonObject.getString("id"), jsonObject.toString());
            }
        }
    }
}
