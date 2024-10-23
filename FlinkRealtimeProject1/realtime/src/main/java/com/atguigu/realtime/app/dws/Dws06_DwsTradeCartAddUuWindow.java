package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.CartAddUuBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: yuan.xin
 * @createTime: 2024/10/22 21:40
 * @contact: yuanxin9997@qq.com
 * @description: 10.6 交易域加购各窗口汇总表
 * 10.6.1 主要任务
 * 	从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 Doris。
 * 10.6.2 思路分析
 * 1）从 Kafka 加购明细主题读取数据
 * 2）转换数据结构
 * 	将流中数据由 String 转换为 JSONObject。
 * 3）设置水位线
 * 4）按照用户 id 分组
 * 5）过滤独立用户加购记录
 * 	运用 Flink 状态编程，将用户末次加购日期维护到状态中。
 * 	如果末次登陆日期为 null 或者不等于当天日期，则保留数据并更新状态，否则丢弃，不做操作。
 * 6）开窗、聚合
 * 	统计窗口中数据条数即为加购独立用户数，补充窗口起始时间、关闭时间，将时间戳字段置为当前系统时间，发送到下游。
 * 7）将数据写入 Doris。
 *
 *
 * drop table if exists dws_trade_cart_add_uu_window;
 * create table if not exists dws_trade_cart_add_uu_window
 * (
 *     `stt`            DATETIME comment '窗口起始时间',
 *     `edt`            DATETIME comment '窗口结束时间',
 *     `cur_date`       DATE comment '当天日期',
 *     `cart_add_uu_ct` BIGINT replace comment '加购独立用户数'
 * ) engine = olap aggregate key (`stt`, `edt`, `cur_date`)
 * comment "交易域加购各窗口汇总表"
 * partition by range(`cur_date`)()
 * distributed by hash(`stt`) buckets 10 properties (
 *   "replication_num" = "3",
 *   "dynamic_partition.enable" = "true",
 *   "dynamic_partition.time_unit" = "DAY",
 *   "dynamic_partition.start" = "-1",
 *   "dynamic_partition.end" = "1",
 *   "dynamic_partition.prefix" = "par",
 *   "dynamic_partition.buckets" = "10",
 *   "dynamic_partition.hot_partition_num" = "1"
 * );
 */
public class Dws06_DwsTradeCartAddUuWindow extends BaseAppV1 {
    public static void main(String[] Args) {
        new Dws06_DwsTradeCartAddUuWindow().init(40006, 2,
                "Dws06_DwsTradeCartAddUuWindow",
                Constant.TOPIC_DWD_TRADE_CART_ADD
                );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // stream.print("Dws06_DwsTradeCartAddUuWindow");
        stream
                .map(JSON::parseObject)
                // 找到用户每个用户的第一条加购记录
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {

                    private ValueState<String> lastAddCartState;  // 最后一次加购状态

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastAddCartState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAddCartState", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context ctx,
                                               Collector<CartAddUuBean> out) throws Exception {
                        Long ts = obj.getLong("ts") * 1000;
                        String today = AtguiguUtil.toDate(ts);
                        if (!today.equals(lastAddCartState.value())) {
                            // 这个用户今天第一次加购
                            lastAddCartState.update(today);
                            out.collect(new CartAddUuBean(
                                    "",
                                    "",
                                    "",
                                    1L,
                                    ts
                            ));
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getTs())
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds((5))))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                            @Override
                            public CartAddUuBean reduce(CartAddUuBean bean1, CartAddUuBean bean2) throws Exception {
                                bean1.setCartAddUuCt(bean1.getCartAddUuCt() + bean2.getCartAddUuCt());
                                return bean1;
                            }
                        },
                        new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>.Context context, Iterable<CartAddUuBean> elements, Collector<CartAddUuBean> out) throws Exception {
                                CartAddUuBean bean = elements.iterator().next();
                                bean.setStt(AtguiguUtil.toDateTime(context.window().getStart()));
                                bean.setEdt(AtguiguUtil.toDateTime(context.window().getEnd()));
                                bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
                                out.collect(bean);
                            }
                        }
                ).map(
                        bean->{
                            SerializeConfig config = new SerializeConfig();
                            config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;  // 转成josn的时候，属性名使用下划线
                            String json = JSON.toJSONString(bean, config);
                            System.out.println(json);
                            return JSON.toJSONString(bean, config);
                        }
                )
                .addSink(FlinkSinkUtil.getDoriSink("gmall2022.dws_trade_cart_add_uu_window"))
                ;
    }
}


/**
 * todo 报错
 * {"cart_add_uu_ct":13,"cur_date":"2024-10-23","edt":"2024-10-22 20:31:50","stt":"2024-10-22 20:31:45","ts":1729600309000}
 * 1    [Map -> Sink: Unnamed (2/2)#54] ERROR org.apache.doris.flink.table.DorisDynamicOutputFormat  - doris sink error, retry times = 0
 * org.apache.doris.flink.exception.StreamLoadException: stream load error: errCode = 2, detailMessage = data cannot be inserted into table with empty partition. Use `SHOW PARTITIONS FROM dws_trade_cart_add_uu_window` to see the currently partitions of this table. , see more in null
 * 	at org.apache.doris.flink.table.DorisStreamLoad.load(DorisStreamLoad.java:109)
 * 	at org.apache.doris.flink.table.DorisDynamicOutputFormat.flush(DorisDynamicOutputFormat.java:319)
 * 	at org.apache.doris.flink.table.DorisDynamicOutputFormat.close(DorisDynamicOutputFormat.java:291)
 * 	at org.apache.doris.flink.cfg.GenericDorisSinkFunction.close(GenericDorisSinkFunction.java:67)
 * 	at org.apache.flink.api.common.functions.util.FunctionUtils.closeFunction(FunctionUtils.java:41)
 * 	at org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.dispose(AbstractUdfStreamOperator.java:117)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.disposeAllOperators(StreamTask.java:864)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.runAndSuppressThrowable(StreamTask.java:843)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.cleanUpInvoke(StreamTask.java:756)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.runWithCleanUpOnFail(StreamTask.java:662)
 * 	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:623)
 * 	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:779)
 */