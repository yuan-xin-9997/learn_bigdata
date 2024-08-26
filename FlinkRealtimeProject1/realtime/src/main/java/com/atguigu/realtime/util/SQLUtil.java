package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/21 21:45
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL Util 工具类
 */
public class SQLUtil {
    public static void main(String[] Args) {

    }

    public static String getKafkaSource(String topic, String groupId) {
        return "with(" +
                " 'connector' = 'kafka'," +
                " 'properties.bootstrap.servers'='"  + Constant.KAFKA_BROKERS +  " ', " +
                " 'properties.group.id'='" + groupId + "', " +
                " 'topic' = '" + topic + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset' " +
                ")";
    }

    public static String getKafkaSink(String topic) {
        return "with(" +
                " 'connector' = 'kafka'," +
                " 'properties.bootstrap.servers'='"  + Constant.KAFKA_BROKERS +  " ', " +
                " 'topic' = '" + topic + "', " +
                " 'format' = 'json' " +
                ")";
    }
}
