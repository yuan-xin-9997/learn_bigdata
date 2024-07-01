package com.atguigu.chapter7.state;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/01 19:21
 * @contact: yuanxin9997@qq.com
 * @description: Flink BackEnd 状态后端
 *
 * 设置状态后端和checkpoint，有2个位置：
 * 1. Flink配置文件（全局配置状态后端）
 *     flink-conf.yaml
 *       旧的写法：
 *           MemoryStateBackend
 *               内存级别的状态后端(默认),  存储方式:本地状态存储在TaskManager的内存中, checkpoint 存储在JobManager的内存中.
 *               state.backend: jobmanagerstate.checkpoints.dir: hdfs://hadoop102:8020/flink/checkpoint
 *           FsStateBackend
 *               存储方式: 本地状态在TaskManager内存, Checkpoint时, 存储在文件系统(hdfs)中
 *               特点: 拥有内存级别的本地访问速度, 和更好的容错保证
 *               使用场景: 1. 常规使用状态的作业. 例如分钟级别窗口聚合, join等 2. 需要开启HA的作业 3. 可以应用在生产环境中
 *               state.backend: filesystem
 *               state.checkpoints.dir: hdfs://hadoop102:8020/flink-checkpoints
 *           RocksDBStateBackend
 *              将所有的状态序列化之后, 存入本地的RocksDB数据库中.(一种NoSql数据库, KV形式存储)
 *              存储方式: 1. 本地状态存储在TaskManager的RocksDB数据库中(实际是内存+磁盘) 2. Checkpoint在外部文件系统(hdfs)中.
 *              使用场景: 1. 超大状态的作业, 例如天级的窗口聚合 2. 需要开启HA的作业 3. 对读写状态性能要求不高的作业 4. 可以使用在生产环境。
 *              state.backend: rocksdb
 *              state.checkpoints.dir: hdfs://hadoop102:8020/flink-checkpoints
 *       新的写法：
 *           state.backend: hashmap 或 rocksdb
 *           state.checkpoints.dir: hdfs://hadoop102:8020/flink-checkpoints 或者 jobmanager
 *           本地rocksdb + jobmanager这种组合一般不用
 * 2. 代码中
 *            // 内存 memory
 *         // 1. 旧（FLink 版本1.31之前）
 *         // env.setStateBackend(new MemoryStateBackend());  // 默认状态：状态存在本地内存 + checkpoint 存在jobmanager的内存中
 *         // 2. 新
 *         //env.setStateBackend(new HashMapStateBackend());
 *         //env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
 *
 *         // 文件系统 fs
 *         // 1. 旧（FLink 版本1.31之前）
 *         //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ch1"));
 *         // 2. 新
 *         //env.setStateBackend(new HashMapStateBackend());
 *         //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink/ch2");
 *
 *         // rocksdb
 *         // 需要额外导入依赖
 *         // <dependency>
 *         //    <groupId>org.apache.flink</groupId>
 *         //    <artifactId>flink-statebackend-rocksdb_${scala.binary.version}</artifactId>
 *         //    <version>${flink.version}</version>
 *         //</dependency>
 *         // 1. 旧（FLink 版本1.31之前）
 *         // env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/ch3_rocksdb"));
 *         // 2. 新
 *         env.setStateBackend(new EmbeddedRocksDBStateBackend());
 *         env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink/ch3");
 */
public class Flink10_State_Backend {
    public static void main(String[] Args) throws IOException {

        // 设置Hadoop 环境变量，IDEA 快捷键 ctrl sht u 将选中的字符设置为大写/小写
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // 开启checkpoint 保存快照，周期：2000s，每个两秒钟把快照持久化一份
        env.enableCheckpointing(2000);

        // 设置状态后端 的 写法

        // 内存 memory
        // 1. 旧（FLink 版本1.31之前）
        // env.setStateBackend(new MemoryStateBackend());  // 默认状态：状态存在本地内存 + checkpoint 存在jobmanager的内存中
        // 2. 新
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // 文件系统 fs
        // 1. 旧（FLink 版本1.31之前）
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ch1"));
        // 2. 新
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink/ch2");

        // rocksdb
        // 需要额外导入依赖
        // <dependency>
        //    <groupId>org.apache.flink</groupId>
        //    <artifactId>flink-statebackend-rocksdb_${scala.binary.version}</artifactId>
        //    <version>${flink.version}</version>
        //</dependency>
        // 1. 旧（FLink 版本1.31之前）
        // env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/ch3_rocksdb"));
        // 2. 新
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink/ch3");

        // Flink程序主逻辑
        // 案例5:MapState
        //去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 9999);
        dataStream
                .map(line->{
                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private MapState<Integer, WaterSensor> vcMapState;  // 键控状态成员变量
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("open");  // 只会在开头执行一次
                        vcMapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Integer, WaterSensor>(
                                        "vcMapState",
                                        //Integer.class,
                                        //WaterSensor.class
                                        TypeInformation.of(new TypeHint<Integer>() {
                                        }),
                                        TypeInformation.of(new TypeHint<WaterSensor>() {
                                        })
                                )
                        );

                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        System.out.println("processElement");  // 每流入一个元素，函数执行一次
                        // 处理输入元素
                        vcMapState.put(value.getVc(), value);
                        // 输出去重的视为
                        Iterable<Integer> vcs = vcMapState.keys();
                        List<Integer> list = AtguiguUtil.toList(vcs);
                        out.collect(ctx.getCurrentKey() + "水位值去重后的结果为: " + list);

                    }
                })
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
