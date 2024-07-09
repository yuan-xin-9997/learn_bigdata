package com.atguigu.chapter07.state;

import com.atguigu.chapter05_source.WaterSensor;
import com.atguigu.utils.AtguiguUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024年6月26日19:48:37
 * @contact: yuanxin9997@qq.com
 * @description: Flink状态-Managed State-Keyed State(键控状态)-ListState列表状态

 * 键控状态是根据输入数据流中定义的键（key）来维护和访问的。
 * Flink为每个键值维护一个状态实例，并将具有相同键的所有数据，都分区到同一个算子任务中，这个任务会维护和处理这个key对应的状态。当任务处理一
 * 条数据时，它会自动将状态的访问范围限定为当前数据的key。因此，具有相同key的所有数据都会访问相同的状态。
 * Keyed State很类似于一个分布式的key-value map数据结构，只能用于KeyedStream（keyBy算子处理之后）。
 *
 * ValueState<T>
 * 保存单个值. 每个key有一个状态值.  设置使用 update(T), 获取使用 T value()
 * ListState<T>:
 * 保存元素列表.
 * 添加元素: add(T)  addAll(List<T>)
 * 获取元素: Iterable<T> get()
 * 覆盖所有元素: update(List<T>)
 * ReducingState<T>:
 * 存储单个值, 表示把所有元素的聚合结果添加到状态中.  与ListState类似, 但是当使用add(T)的时候ReducingState会使用指定的ReduceFunction进行聚合.
 * AggregatingState<IN, OUT>:
 * 存储单个值. 与ReducingState类似, 都是进行聚合. 不同的是, AggregatingState的聚合的结果和元素类型可以不一样.
 * MapState<UK, UV>:
 * 存储键值对列表.
 * 添加键值对:  put(UK, UV) or putAll(Map<UK, UV>)
 * 根据key获取值: get(UK)
 * 获取所有: entries(), keys() and values()
 * 检测是否为空: isEmpty()
 * 注意:
 * a)所有的类型都有clear(), 清空当前key的状态
 * b)这些状态对象仅用于用户与状态进行交互.
 * c)状态不是必须存储到内存, 也可以存储在磁盘或者任意其他地方
 * d)从状态获取的值与输入元素的key相关
 */
public class Flink05_State_KeyedState_ListState {

    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // 开启checkpoint 保存快照，周期：2000s，每个两秒钟把快照持久化一份
        env.enableCheckpointing(2000);

        // Flink程序主逻辑

        // 案例2: ListState
        //针对每个传感器输出最高的3个水位值
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 9999);
        dataStream
                .map(line->{
                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ListState<Integer> top3VcState;  // 键控状态成员变量

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取键控状态
                        top3VcState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3VcState", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        // 将当前水位的值添加到状态中
                        top3VcState.add(value.getVc());
                        // 取出状态中的值
                        Iterable<Integer> it = top3VcState.get();
                        List<Integer> list = AtguiguUtil.toList(it);
                        // 排序
                        //list.sort(new Comparator<Integer>() {
                        //    @Override
                        //    public int compare(Integer o1, Integer o2) {
                        //        return o2.compareTo(o1);  // 如果o1==o2，则返回0，o1>o2，返回>0的值，否则返回<0的值
                        //    }
                        //});
                        //list.sort((o1, o2) -> o2.compareTo(o1));
                        list.sort(Comparator.reverseOrder());  // 降序
                        // 取出排序前3的元素
                        if (list.size()==4) {
                            list.remove(list.size() - 1);  // 移除最后1个元素
                        }
                        top3VcState.update(list); // 覆盖更新状态
                        out.collect(list.toString());
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
