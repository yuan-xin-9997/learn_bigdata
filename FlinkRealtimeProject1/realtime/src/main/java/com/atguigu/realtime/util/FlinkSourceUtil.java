package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
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
        return new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {

                /**
                 * 反序列化之后的数据类型
                 * @return
                 */
                @Override
                public TypeInformation<String> getProducedType() {
                    return TypeInformation.of(new TypeHint<String>() {});
                }

                    /**
                     * 读取到nextElement之后，是否是结束流
                     * @param nextElement The element to test for the end-of-stream signal.
                     * @return
                     */
                @Override
                public boolean isEndOfStream(String nextElement) {
                    return false;
                }

                    /**
                     * 反序列化方法
                     * @param record Kafka record to be deserialized.
                     * @return
                     * @throws Exception
                     */
                @Override
                public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                    byte[] value = record.value();// 获取从kafka中消费的数据的value值
                    if (value!=null) {
                        return new String(value, StandardCharsets.UTF_8);
                    }
                    return null;
                }
        },
                properties);
    }
}
