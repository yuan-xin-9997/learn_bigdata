package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/19 21:25
 * @contact: yuanxin9997@qq.com
 * @description: 9.2 流量域独立访客事务事实表
 *
 * 流量域独立访客事务事实表
 * 9.2.1 主要任务
 * 	过滤页面数据中的独立访客访问记录。
 * 9.2.2 思路分析
 * 1）过滤 last_page_id 不为 null 的数据
 * 	独立访客数据对应的页面必然是会话起始页面，last_page_id 必为 null。过滤 last_page_id != null 的数据，减小数据量，提升计算效率。
 * 2）筛选独立访客记录
 * 	运用 Flink 状态编程，为每个 mid 维护一个键控状态，记录末次登录日期。
 * 	如果末次登录日期为 null 或者不是今日，则本次访问是该 mid 当日首次访问，保留数据，将末次登录日期更新为当日。否则不是当日首次访问，丢弃数据。
 * 3）状态存活时间设置
 * 	如果保留状态，第二日同一 mid 再次访问时会被判定为新访客，如果清空状态，判定结果相同，所以只要时钟进入第二日状态就可以清空。
 * 设置状态的 TTL 为 1 天，更新模式为 OnCreateAndWrite，表示在创建和更新状态时重置状态存活时间。如：2022-02-21 08:00:00 首次访问，若 2022-02-22 没有访问记录，则 2022-02-22 08:00:00 之后状态清空。
 *
 */
public class Dwd_02_DwdTrafficUniqueVisitorDetail extends BaseAppV1 {
    /**
     *
     * dwd去重
     *      写出每个用户的当天的第一条明细数据
     * 数据源：
     *      启动目志
     *          可以，但是，数据量可能偏小
     *          只有app有
     *      页面
     *          只要找到第一个页面记录
     * 如何找到第一个访问记录？
     *      使用状态
     *      如果考虑乱序，应该找到第一个窗口，窗口内的时间戳最小的那个
     *
     * @param Args
     */
    public static void main(String[] Args) {
        new Dwd_02_DwdTrafficUniqueVisitorDetail().init(
                3002,
                2,
                "Dwd_02_DwdTrafficUniqueVisitorDetail",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // stream.print();
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(  // 添加水印，事件时间
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, timestamp) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))  // 开窗，滚动窗口
                .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {

                    private ValueState<String> visitDateState;  // 状态：记录用户第一次访问的日期

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        visitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitDateState", String.class));
                    }

                    @Override
                    public void process(String s, ProcessWindowFunction<JSONObject, String, String, TimeWindow>.Context ctx, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
                        // 找到用户当天的第一个窗口
                        String date = visitDateState.value();
                        String today = AtguiguUtil.toDate(ctx.window().getStart());
                        if (!today.equals(date)) { // 今天和状态的日期不等，则表示当天的第一个窗口
                            List<JSONObject> list = AtguiguUtil.toList(elements);
                            JSONObject min = Collections.min(list, (o1, o2) -> o2.getLong("ts").compareTo(o1.getLong("ts")));// 找到窗口内时间最小的
                            out.collect(min.toJSONString());
                            // 更新状态
                            visitDateState.update(today);
                        }
                    }
                })
                .print()
        ;
    }
}

















