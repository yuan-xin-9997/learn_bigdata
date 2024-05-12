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
 *
 * KafkaProducerUtil是单例。
 *
 * 单例怎么写？
 *  static producer
 *  static {
 *      // 为静态属性赋值
 *  }
 * 不是单例的怎么写？
 *      以分区为单位去new KafkaProducerUtil2()
 *      KafkaProducerUtil2 client = new KafkaProducerUtil2()
 *      client.sendData()
 *
 * ----------------------------------
 * 什么时候需要设置单例？如果希望一个JVM中只能new 一个(1次）这个对象，设置为单例
 *      典型的是那些消耗资源、消耗时间的操作
 *      举例：HBase Connection  一个App有一个Connection就可以设置为单例
 *
 * -------------------------------------------
 * KafkaProducerUtil是单例和不是单例的区别？
 *      以分区为单位使用，生产者发送数据
 *      功能上没区别，非单例的，需要创建更多的Producer，需要更多的资源（缓冲区）
 */
public class KafkaProducerUtil2 {
    private Producer<String,String> producer;

    {
        producer = createProducer();
    }

    /**
     * 创建生产者
     * @return
     */
    private Producer<String,String> createProducer(){
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
    public void sendData(String message, String topic){
        producer.send(new ProducerRecord<String, String>(topic, message));
    }

    /**
     * 生产的数据先进入buffer，满足一定条件（缓冲区大小、时间间隔）才会发到broker
     */
    public void flush(){
        producer.flush();
    }

    public static void main(String[] Args) {
        KafkaProducerUtil2 kafkaProducerUtil2 = new KafkaProducerUtil2();
        System.out.println(kafkaProducerUtil2);

    }
}
