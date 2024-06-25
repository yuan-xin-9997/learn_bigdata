package com.atguigu.chapter7.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/25 20:12
 * @contact: yuanxin9997@qq.com
 * @description: Flink状态-Managed State-Operator State(算子状态)-Broadcast State广播状态
 * <p>
 * Operator State可以用在所有算子上，每个算子子任务或者说每个算子实例共享一个状态，流入这个算子子任务的数据可以访问和更新这个状态。
 * <p>
 * 注意: 算子子任务之间的状态不能互相访问
 * Operator State的实际应用场景不如Keyed State多，它经常被用在Source或Sink等算子上，用来保存流入数据的偏移量或对输出数据做缓存，以保证Flink应用的Exactly-Once语义。
 * Flink为算子状态提供三种基本数据结构：
 * 列表状态（List state）
 * 将状态表示为一组数据的列表
 * <p>
 * 联合列表状态（Union list state）
 * 也是将状态表示为数据的列表。它与常规列表状态的区别在于，在发生故障时，或者从保存点（savepoint）启动应用程序时如何恢复。
 * 一种是均匀分配(List state)，另外一种是将所有 State 合并为全量 State 再分发给每个实例(Union list state)。
 * <p>
 * 广播状态（Broadcast state）
 * 是一种特殊的算子状态. 如果一个算子有多项任务，而它的每项任务状态又都相同，那么这种特殊情况最适合应用广播状态。
 * <p>
 * <p>
 * ！！！注意！！！：只有程序遇到异常，Flink会自行重启，状态才能恢复。如果是自己kill程序，再启动程序，状态就无法恢复。
 * <p>
 * -------------------
 * 案例2: 广播状态
 * 从版本1.5.0开始，Apache Flink具有一种新的状态，称为广播状态。
 * 广播状态被引入以支持这样的用例:来自一个流的一些数据需要广播到所有下游任务，在那里它被本地存储，并用于处理另一个流上的所有传入元素。作为广播状态自然适合出现的一个例子，我们可以想象一个低吞吐量流，其中包含一组规则，我们希望根据来自另一个流的所有元素对这些规则进行评估。考虑到上述类型的用例，广播状态与其他算子状态的区别在于:
 * 1. 它是一个map格式
 * 2. 它只对输入有广播流和无广播流的特定算子可用
 * 3. 这样的算子可以具有不同名称的多个广播状态。
 */
public class Flink03_State_OperatorState_BroadcastState {
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
        // 获取一个数据流
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 8888);
        // 获取一个配置流
        DataStreamSource<String> configStream = env.socketTextStream("hadoop102", 9999);

        // 1. 把配置流做成广播流
        MapStateDescriptor<String, String> bcStateDescriptor = new MapStateDescriptor<>("bcStateDescriptor", String.class, String.class);
        BroadcastStream<String> bcStream = configStream.broadcast(bcStateDescriptor);

        // 2. 把数据流和广播流进行连接connect(让数据流connect广播流，广播流没有方法，只能被动connect）
        BroadcastConnectedStream<String, String> connectedStream = dataStream.connect(bcStream);
        connectedStream.process(new BroadcastProcessFunction<String, String, String>() {
                    /**
                     * 4. 处理广播数据流中的数据：从广播状态中取配置
                     * @param value The stream element.
                     * @param ctx A {@link ReadOnlyContext} that allows querying the timestamp of the element,
                     *     querying the current processing/event time and updating the broadcast state. The context
                     *     is only valid during the invocation of this method, do not store it.
                     * @param out The collector to emit resulting elements to
                     * @throws Exception
                     */
                    @Override
                    public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(bcStateDescriptor);
                    }

                    /**
                     * 3. 把广播流中的数据放入到广播状态
                     * @param value The stream element.
                     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
                     *     current processing/event time and updating the broadcast state. The context is only valid
                     *     during the invocation of this method, do not store it.
                     * @param out The collector to emit resulting elements to
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(String value,  // 配置流中的数据
                                                        BroadcastProcessFunction<String, String, String>.Context ctx,//上下文
                                                        Collector<String> out) throws Exception {
                        // 获取广播状态，把配置流信息写入到广播状态中
                        BroadcastState<String, String> state = ctx.getBroadcastState(bcStateDescriptor);
                        state.put("aSwitch", value);
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
         *
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
