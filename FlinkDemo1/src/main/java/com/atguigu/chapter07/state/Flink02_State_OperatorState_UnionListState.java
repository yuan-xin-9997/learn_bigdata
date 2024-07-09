package com.atguigu.chapter07.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/25 20:12
 * @contact: yuanxin9997@qq.com
 * @description: Flink状态-Managed State-Operator State(算子状态)-Union list state 联合列表状态
 *
 * Operator State可以用在所有算子上，每个算子子任务或者说每个算子实例共享一个状态，流入这个算子子任务的数据可以访问和更新这个状态。
 *
 * 注意: 算子子任务之间的状态不能互相访问
 * Operator State的实际应用场景不如Keyed State多，它经常被用在Source或Sink等算子上，用来保存流入数据的偏移量或对输出数据做缓存，以保证Flink应用的Exactly-Once语义。
 * Flink为算子状态提供三种基本数据结构：
 * 列表状态（List state）
 * 将状态表示为一组数据的列表
 *
 * 联合列表状态（Union list state）
 * 也是将状态表示为数据的列表。它与常规列表状态的区别在于，在发生故障时，或者从保存点（savepoint）启动应用程序时如何恢复。
 * 一种是均匀分配(List state)，另外一种是将所有 State 合并为全量 State 再分发给每个实例(Union list state)。
 * 
 * 广播状态（Broadcast state）
 * 是一种特殊的算子状态. 如果一个算子有多项任务，而它的每项任务状态又都相同，那么这种特殊情况最适合应用广播状态。
 *
 *
 * ！！！注意！！！：只有程序遇到异常，Flink会自行重启，状态才能恢复。如果是自己kill程序，再启动程序，状态就无法恢复。
 */
public class Flink02_State_OperatorState_UnionListState {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(2);

        // 开启checkpoint 保存快照，周期：2000s，每个两秒钟把快照持久化一份
        env.enableCheckpointing(2000);

        // Flink程序主逻辑
        // 把这些单词存入状态中，当程序重启时候，可以把状态中的数据恢复
        env
                .socketTextStream("hadoop102", 9999)
                .map(new MyMapFunction())
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    // Operator State 需要实现 实现CheckpointedFunction接口
    public static class MyMapFunction implements MapFunction<String, String>, CheckpointedFunction {

        List<String> words = new ArrayList<String>();  // 读取的数据
        private ListState<String> wordsState;  // 列表状态

        @Override
        public String map(String line) throws Exception {

            // 手动抛出异常
            if (line.contains("x")) {
                throw new RuntimeException("包含x，手动抛出异常");
            }

            String[] data = line.split(" ");
            words.addAll(Arrays.asList(data));
            return words.toString();
        }

        /**
         * 保存状态，此函数会周期性的执行
         * 每个并行度都会周期性的执行
         *
         * @param ctx the context for drawing a snapshot of the operator
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            // 把数据存入到算子状态（列表状态）
            //wordsState.clear(); // 清空状态
            //wordsState.addAll(words);  // 把数据存入到列表状态中
            wordsState.update(words);  // 更新状态=wordsState.clear()+wordsState.addAll()

            // System.out.println("snapshotState" + System.currentTimeMillis());
        }

        /**
         * 初始化状态
         * 程序启动的时候，每个并行度执行一次
         * 可以把状态中的数据恢复到Java集合中
         * @param ctx the context for initializing the operator
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            // 从状态恢复数据    提升成员变量快捷键：Ctrl + alt + f
            // 获取列表状态
            wordsState = ctx.getOperatorStateStore().getUnionListState(new ListStateDescriptor<String>("wordsState", String.class));
            // 从列表状态中获取数据
            Iterable<String> it = wordsState.get();
            for (String word : it) {
                words.add(word);
            }
            System.out.println("initializeState" + System.currentTimeMillis());
        }
    }

}
