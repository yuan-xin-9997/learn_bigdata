package com.atguigu.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomerPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 1. 根据key计算分区号
        // 1.1 判断是否存在key，如果没有传入分区，则会报空指针异常，老师课堂上讲错了
        System.out.println("key=" + key.toString());

        // 1. 获取解析key
        String keyStr = key.toString();
        // 2. 计算key的哈希值
        int keyHashCode = keyStr.hashCode();
        // 3. 获取分区个数
        Integer partitionNumber = cluster.partitionCountForTopic(topic);
        // 4. 计算分区
        int partition = Math.abs(keyHashCode) % partitionNumber;
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
