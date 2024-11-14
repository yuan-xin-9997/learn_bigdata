package com.atguigu.realtime.app;

import com.atguigu.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024年10月10日19:10:42
 * @contact: yuanxin9997@qq.com
 * @description: Flink 流处理 基座App - 消费多个topic版本
 */
public abstract class BaseAppV2 {
    public static void main(String[] Args) {

    }

    protected abstract void handle(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> streams);

    public void init(int port, int parallelization, String ckPathAndGroupIdAndJobName, String ... topics){

        if (topics.length == 0) {
            throw new RuntimeException("请至少指定一个topic");
        }

        // 设置环境变量
        setProperty("HADOOP_USER_NAME", "atguigu");

        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(parallelization);

        // Flink程序主逻辑

        // 设置参数
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/" + ckPathAndGroupIdAndJobName);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1200 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 遍历topics得到多个流
        HashMap<String, DataStreamSource<String>> streams = new HashMap<String, DataStreamSource<String>>();
        for (String topic : topics) {
            // 从Kafka读取数据
            DataStreamSource<String> stream = env.addSource(
                    FlinkSourceUtil.getKafkaSource(ckPathAndGroupIdAndJobName, topic)
            );
            streams.put(topic, stream);
        }

        // 调用抽象方法
        handle(env, streams);

        // 懒加载
        try {
            env.execute(ckPathAndGroupIdAndJobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
