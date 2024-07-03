package com.atguigu.chapter7.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/02 20:05
 * @contact: yuanxin9997@qq.com
 * @description: 7.9.5Kafka+Flink+Kafka 实现端到端严格一次
 *
 * 7.9.5Kafka+Flink+Kafka 实现端到端严格一次
 * 我们知道，端到端的状态一致性的实现，需要每一个组件都实现，对于Flink + Kafka的数据管道系统（Kafka进、Kafka出）而言，各组件怎样保证exactly-once语义呢？
 * 内部 —— 利用checkpoint机制，把状态存盘，发生故障的时候可以恢复，保证部的状态一致性
 * source —— kafka consumer作为source，可以将偏移量保存下来，如果后续任务出现了故障，恢复的时候可以由连接器重置偏移量，重新消费数据，保证一致性
 * sink —— kafka producer作为sink，采用两阶段提交 sink，需要实现一个 TwoPhaseCommitSinkFunction
 * 	内部的checkpoint机制我们已经有了了解，那source和sink具体又是怎样运行的呢？接下来我们逐步做一个分析。
 *
 * 具体的两阶段提交步骤总结如下：
 * 1)某个checkpoint的第一条数据来了之后，开启一个 kafka 的事务（transaction），正常写入 kafka 分区日志但标记为未提交，这就是“预提交”(第一阶段提交)
 * 2)jobmanager 触发 checkpoint 操作，barrier 从 source 开始向下传递，遇到 barrier 的算子状态后端会进行相应进行checkpoint，并通知 jobmanagerr
 * 3)sink 连接器收到 barrier，保存当前状态，存入 checkpoint，通知 jobmanager，并开启下一阶段的事务，用于提交下个检查点的数据
 * 4)jobmanager 收到所有任务的通知，发出确认信息，表示 checkpoint 完成
 * 5)sink 任务收到 jobmanager 的确认信息，正式提交这段时间的数据(第二阶段提交)
 * 6)外部kafka关闭事务，提交的数据可以正常消费了
 *
 * 注意：
 * Kafka 消费s2主题时候
 * bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --isolation-level  read_committed  --topic s2
 * 需要增加 --isolation-level  read_committed
 *
--isolation-level <String>               Set to read_committed in order to
                                           filter out transactional messages
                                           which are not committed. Set to
                                           read_uncommitted to read all
                                           messages. (default: read_uncommitted)
 */
public class Flink11_Kafka_Flink_Kafka_end_to_end_exactlyonce {
    public static void main(String[] Args) {

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // 开启checkPoint检查点
        env.enableCheckpointing(2000);  // 设置checkPoint的时间间隔，每隔2000ms执行一次
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink/ck100");  // 设置checkPoint目录
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  // 设置checkpoint检查点模式，严格一次
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);  // 限制同时进行的checkpoint的数量的上限
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);  // 设置两次checkpoint的最小时间间隔(为了减少对HDFS的压力)
        // 1.13.6新增的方法：当程序被ctrl c的时候，保留HDFS中的checkpoint数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  // 设置当作业被取消时，是否保留checkpoint数

        // Flink程序主逻辑
        Properties souceProps = new Properties();
        souceProps.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        souceProps.put("group.id", "Flink11_Kafka_Flink_Kafka_end_to_end_exactlyonce");
        // souceProps.put("isolation.level", "read_committed");  // 消费开启事务的topic的时候，只消费已commit的数据，防止重复读取数据（至少一次）
        // souceProps.put("auto.offset.reset", "latest");
        Properties sinkProps = new Properties();
        sinkProps.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        // 如果报错The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
        // 则参照如下修改
        // https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/#kafka-producers-and-fault-tolerance
        // Kafka brokers by default have transaction.max.timeout.ms set to 15 minutes. This property will not allow to
        // set transaction timeouts for the producers larger than it’s value. FlinkKafkaProducer by default sets the
        // transaction.timeout.ms property
        // in producer config to 1 hour, thus transaction.max.timeout.ms should be increased before using
        // the Semantic.EXACTLY_ONCE mode.
        sinkProps.put("transaction.timeout.ms", 15 * 60 * 1000);  // 设置事务超时时间为15分钟，不大于kafka broker设置的时间，这个值flink默认是1h，kafka broker默认是15分钟
        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env
                .addSource(  // Kafka source
                        new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), souceProps)
                                .setStartFromLatest()  // 当启动的时候，如果没有消费记录，则从最新的开始消费，支持重读
                )
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] split = value.split(" ");
                        for (String s : split) {
                            out.collect(Tuple2.of(s, 1L));
                        }
                    }
                })
                .keyBy(r -> r.f0)
                .sum(1);//.print();
        stream
                .addSink(   // Kafka sink
                        new FlinkKafkaProducer<Tuple2<String, Long>>(
                                "default",  // 没有使用
                                new KafkaSerializationSchema<Tuple2<String, Long>>() {
                                    @Override
                                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> element, @Nullable Long timestamp) {
                                        return new ProducerRecord<>("s2", (element.f0 + "_" + + element.f1).getBytes(StandardCharsets.UTF_8));
                                    }
                                },
                                sinkProps,
                                FlinkKafkaProducer.Semantic.EXACTLY_ONCE  // 语义：严格一次
                        )
                );

        stream.addSink(new SinkFunction<Tuple2<String, Long>>() {
            @Override
            public void invoke(Tuple2<String, Long> value) throws Exception {
                if (value.f0.contains("x")) {
                    throw new RuntimeException("[ERROR] x");
                }
            }
        });

        // 懒加载
        try {
            env.execute("Flink11_Kafka_Flink_Kafka_end_to_end_exactlyonce");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
