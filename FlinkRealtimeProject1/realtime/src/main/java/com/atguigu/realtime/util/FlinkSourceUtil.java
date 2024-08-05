package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/31 20:17
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class FlinkSourceUtil {
    public static void main(String[] Args) {

    }


    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        properties.setProperty("isolation.level", "read_committed");
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }
}
