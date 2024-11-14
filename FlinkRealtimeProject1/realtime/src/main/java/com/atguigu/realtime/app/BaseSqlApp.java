package com.atguigu.realtime.app;

import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSourceUtil;
import com.atguigu.realtime.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/31 20:45
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL 基座 App
 */
public abstract class BaseSqlApp {
    public static void main(String[] Args) {

    }

    public void init(int port, int parallelization, String ckPathAndJobName){
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
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/" + ckPathAndJobName);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1200 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 调用抽象方法
        handle(env, tableEnvironment);

        // 懒加载  纯SQL文件用不上
        // try {
        //     env.execute(ckPathAndJobName);
        // } catch (Exception e) {
        //     throw new RuntimeException(e);
        // }
    }

    /**
     * 需要由子类实现的抽象方法 - 用来处理业务逻辑
     * @param env
     * @param tEnv
     */
    protected abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);

    /**
     * 读取Kafka Topic数据（ODS_DB)的数据
     * @param tEnv
     */
    public void readOdsDb(StreamTableEnvironment tEnv, String groupId){
        tEnv.executeSql("create table ods_db(" +
                " `database` string, " +
                " `table` string, " +
                " `type` string, " +
                " `ts` bigint, " +
                " `data` map<string, string>, " +
                " `old` map<string, string>, " +
                " `pt` as proctime() " +  // 增加处理时间字段，供后面 lookup join 的时候使用
                ") " + SQLUtil.getKafkaSource(Constant.TOPIC_ODS_DB, groupId))
                ;
    }

    /**
     * 读取字典表
     * 基于 Lookup Join(Flink SQL实现 事实表join维度表) 实现
     * @param tEnv
     */
    public void readBaseDic(StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE base_dic(" +
                "  dic_code string, " +
                "  dic_name string " +
                ") WITH (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false', " +
                " 'table-name' = 'base_dic', " +
                " 'username' = 'root', " +
                " 'password' = 'aaaaaa'," +
                " 'lookup.cache.max-rows'='10', " +
                " 'lookup.cache.ttl'='1 hour' " +
                ")");
    }

}
