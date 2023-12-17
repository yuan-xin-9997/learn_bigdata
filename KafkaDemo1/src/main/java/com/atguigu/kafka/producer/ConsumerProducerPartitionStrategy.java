package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerProducerPartitionStrategy {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. 创建配置对象
        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , "hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. 创建kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 3. 发送数据
        for (int i=1;i<11;i++){

            // 4. 造数据
            String message="你好，客官，我是"+i+"号，很高兴为您服务";

            // 5. 创建producerRecord，指定分区策略
            //粘性分区
            // final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first", message);
            // 指定分区
            // final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first", 0, "", message);
            // 对key进行hash决定分区
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first", i+"", message);

            System.out.println("异步：你看看我在哪里");

            // 创建回调对象
            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // 6.1 当发送成功时，输出元数据信息
                    if (e == null) {
                        long offset = recordMetadata.offset();
                        int partition = recordMetadata.partition();
                        System.out.println(producerRecord.value() + " 分区: " + partition + ", 偏移量：" + offset);
                    } else {
                        System.out.println("发送失败");
                    }
                }
            };

            // 6. 发送数据
            producer.send(producerRecord, callback);
        }

        // 7. 关闭资源
        producer.close();
    }
}
