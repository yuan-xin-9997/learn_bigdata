package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * 普通异步发送
 * 1. 需求：创建Kafka生产者，采用异步的方式发送到Kafka broker
 */
public class CustomProducerWithCustomerPartition {
    public static void main(String[] args) {
        // 1. 创建配置对象
        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , "hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // =================设置自定义分区类=================
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.partitioner.CustomerPartitioner");

        // 2. 创建kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 3. 发送数据
        for (int i=1;i<11;i++){

            // 4. 造数据
            String message="你好，客官，我是"+i+"号，很高兴为您服务";

            // 5. 创建producerRecord
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                    "first",
//                    i+"",
                    message
            );

            // 6. 发送数据
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // 6.1 当发送成功时，输出元数据信息
                    if(e == null){
                        long offset = recordMetadata.offset();
                        int partition = recordMetadata.partition();
                        System.out.println(producerRecord.value() + " 分区: "+partition + ", 偏移量：" + offset);
                    }else{
                        System.out.println("发送失败");
                    }
                }
            });
        }

        // 7. 关闭资源
        producer.close();
    }
}
