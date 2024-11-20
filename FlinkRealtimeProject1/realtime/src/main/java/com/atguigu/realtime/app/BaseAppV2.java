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

        // 关于Flink的RestartStrategy重启策略：
        // Flink 作业如果没有定义重启策略，则会遵循集群启动时加载的默认重启策略。 如果提交作业时设置了重启策略，该策略将覆盖掉集群的默认策略。
        //
        // 通过 Flink 的配置文件 flink-conf.yaml 来设置默认的重启策略。配置参数 restart-strategy 定义了采取何种策略。 如果没有启用
        // checkpoint，就采用“不重启”策略。如果启用了 checkpoint 且没有配置重启策略，那么就采用固定延时重启策略， 此时最大尝试重启次
        // 数由 Integer.MAX_VALUE 参数设置。
        // ————————————————
        //
        //                             版权声明：本文为博主原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接和本声明。
        //
        // 原文链接：https://blog.csdn.net/qq_44962429/article/details/103685582

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
