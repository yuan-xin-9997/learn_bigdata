package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.sink.PhoenixSink;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/06 21:21
 * @contact: yuanxin9997@qq.com Flink Sink 工具类
 * @description:
 */
public class FlinkSinkUtil {
    public static void main(String[] Args) {}

    /**
     * Flink Phoenix Sink
     * @return
     */
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    }

    /**
     * Flink Kafka Sink
     * @param topic  要写入的topic
     * @return
     */
    public static SinkFunction<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");
        return new FlinkKafkaProducer<String>(
                "default",
                new KafkaSerializationSchema<String>(){
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp){
                        return new ProducerRecord<>(topic, element.getBytes());
                        }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    /**
     * Flink Doris Sink
     * @param tableName
     * @return
     */
    public static SinkFunction<String> getDoriSink(String tableName) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");
        return DorisSink.sink(
                        new DorisExecutionOptions.Builder()
                                .setBatchIntervalMs(2000L)
                                .setBatchSize(1024 * 1024)
                                .setEnableDelete(false)
                                .setMaxRetries(3)
                                .setStreamLoadProp(props)
                                .build(),
                        new DorisOptions.Builder()
                                .setFenodes(Constant.DORIS_FENODES)
                                .setUsername("root")
                                .setPassword("aaaaaa")
                                .setTableIdentifier(tableName)
                                .build()
                );
    }
}
