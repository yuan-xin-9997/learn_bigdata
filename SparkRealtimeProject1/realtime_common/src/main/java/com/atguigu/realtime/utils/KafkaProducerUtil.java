package com.atguigu.realtime.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 12:16
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class KafkaProducerUtil {
    private static Producer<String,String> producer;

    static {
        // 为静态变量赋值
        producer = createProducer();
    }

    /**
     * 创建生产者
     * @return
     */
    private static Producer<String,String> createProducer(){
        Properties properties = new Properties();
        /*
        必须传入的参数：
            1.集群地址
            2. key,value的序列化器

        参考ProducerConfig
         */
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getProperty("kafka.broker.list"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getProperty("key.serializer"));
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getProperty("value.serializer"));
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");  // 开启幂等性？
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        return kafkaProducer;
    }

    // 生产指定的消息到指定的topic
    public static void sendData(String message, String topic){
        producer.send(new ProducerRecord<String, String>(topic, message));
    }

    /**
     * 生产的数据先进入buffer，满足一定条件（缓冲区大小、时间间隔）才会发到broker
     */
    public static void flush(){
        producer.flush();
    }

    public static void main(String[] Args) {

    }
}
