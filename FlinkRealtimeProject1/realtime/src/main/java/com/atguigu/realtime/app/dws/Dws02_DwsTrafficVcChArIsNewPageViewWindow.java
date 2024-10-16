package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.TrafficPageViewBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.collection.Iterable;

import java.time.Duration;
import java.util.HashMap;

/**
 * @author: yuan.xin
 * @createTime: 2024/10/10 19:08
 * @contact: yuanxin9997@qq.com
 * @description: 10.2 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 *
 * 10.2 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 *
 * 计算 版本-渠道-地区-访客类别 粒度下：
 *
 * 会话数
 *     数据源：
 *         页面日志
 *             过滤出last_page_id is null的日志，统计个数
 *
 * 页面浏览数 pv
 *     数据源：
 *         页面日志
 *             直接统计个数
 *
 * 浏览总时长
 *     数据源：
 *         页面日志
 *             直接sum(during_time)
 *
 * 独立访客数 uv
 *     数据源：
 *         uv详情
 *             直接统计个数
 *
 * 跳出会话数：
 *     数据源：
 *         跳出明细
 *             直接统计个数
 *
 * 总结：
 *     5个指标，来源3个流
 *
 *
 * --------------------------------------
 * 10.2.4 Doris 建表语句
 * drop table if exists dws_traffic_vc_ch_ar_is_new_page_view_window;
 * create table if not exists dws_traffic_vc_ch_ar_is_new_page_view_window
 * (
 *     `stt`      DATETIME comment '窗口起始时间',
 *     `edt`      DATETIME comment '窗口结束时间',
 *     `vc`       VARCHAR(10) comment '版本',
 *     `ch`       VARCHAR(10) comment '渠道',
 *     `ar`       VARCHAR(10) comment '地区',
 *     `is_new`   VARCHAR(10) comment '新老访客状态标记',
 *     `cur_date` DATE comment '当天日期',
 *     `uv_ct`    BIGINT replace comment '独立访客数',
 *     `sv_ct`    BIGINT replace comment '会话数',
 *     `pv_ct`    BIGINT replace comment '页面浏览数',
 *     `dur_sum`  BIGINT replace comment '页面访问时长',
 *     `uj_ct`    BIGINT replace comment '跳出会话数'
 * ) engine = olap aggregate key (`stt`, `edt`, `vc`, `ch`, `ar`, `is_new`, `cur_date`)
 * comment "流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表"
 * partition by range(`cur_date`)()
 * distributed by hash(`vc`, `ch`, `ar`, `is_new`) buckets 10 properties (
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
public class Dws02_DwsTrafficVcChArIsNewPageViewWindow extends BaseAppV2 {
    public static void main(String[] Args) {
        new Dws02_DwsTrafficVcChArIsNewPageViewWindow().init(
                40002,
                2,
                "Dws02_DwsTrafficVcChArIsNewPageViewWindow",
                Constant.TOPIC_DWD_TRAFFIC_PAGE,
                Constant.TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL,
                Constant.TOPIC_DWD_TRAFFIC_USER_JUMP_DETAIL
                );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> streams) {
        // 是否能消费到流
        // streams.get(Constant.TOPIC_DWD_TRAFFIC_PAGE).print(Constant.TOPIC_DWD_TRAFFIC_PAGE);
        // streams.get(Constant.TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL).print(Constant.TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL);
        // streams.get(Constant.TOPIC_DWD_TRAFFIC_USER_JUMP_DETAIL).print(Constant.TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL);

        // 1. 把流转成同一种类型，然后union成一个流
        DataStream<TrafficPageViewBean> beanStream = parseAndUnionOne(streams);
        // beanStream.print("beanStream");

        // 2. 开窗聚合
        SingleOutputStreamOperator<TrafficPageViewBean> resultStream = windowAndAgg(beanStream);
        // resultStream.print("normal");
        DataStream<TrafficPageViewBean> sideOutput = resultStream.getSideOutput(new OutputTag<TrafficPageViewBean>("late") {
        });
        // sideOutput.print("late");

        // 3. 写到doris中
        writeToDoris(resultStream);
    }

    private void writeToDoris(SingleOutputStreamOperator<TrafficPageViewBean> resultStream) {
        resultStream
                .map(
                        bean -> {
                            SerializeConfig config = new SerializeConfig();
                            config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;  // 转成josn的时候，属性名使用下划线
                            String json = JSON.toJSONString(bean, config);
                            System.out.println(json);
                            return json;
                        }
                )
                .addSink(FlinkSinkUtil.getDoriSink(
                        "gmall2022.dws_traffic_vc_ch_ar_is_new_page_view_window"
                ))
                ;
    }

    /**
     * 开窗聚合函数
     *
     * 开窗聚合前有uv数据，开窗聚合后，没有uv的数据，为什么？
     *  怀疑uv数据迟到了，窗口关闭了，但是数据还没来
         *  求证是否真的迟到
     *          使用侧输出流，迟到的数据会被放入到侧输出流中
         *  如果真的迟到，找原因
     *           计算uv的时候，有窗口数据来的比较慢
     *  找到解决措施
     *      1. 在每个流的后面加水印
     *          如果一个流中没有数据，会导致水印长时间不更新
     *      2. 增加乱序程度
     * @param beanStream
     * @return
     */
    private SingleOutputStreamOperator<TrafficPageViewBean> windowAndAgg(DataStream<TrafficPageViewBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .keyBy(bean -> bean.getVc() + "_" + bean.getCh() + "_" + bean.getAr() + "_" + bean.getIsNew())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<TrafficPageViewBean>("late"){})
                .reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean bean1,
                                                              TrafficPageViewBean bean2) throws Exception {
                                bean1.setPvCt(bean1.getPvCt() + bean2.getPvCt());
                                bean1.setSvCt(bean1.getSvCt() + bean2.getSvCt());
                                bean1.setUvCt(bean1.getUvCt() + bean2.getUvCt());
                                bean1.setUjCt(bean1.getUjCt() + bean2.getUjCt());
                                bean1.setDurSum(bean1.getDurSum() + bean2.getDurSum());
                                // bean1.setTs(System.currentTimeMillis());
                                return bean1;
                            }
                        }
                        , new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>.Context ctx,
                                                java.lang.Iterable<TrafficPageViewBean> elements,
                                                Collector<TrafficPageViewBean> out) throws Exception {
                                TrafficPageViewBean bean = elements.iterator().next();
                                bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                                bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
                                bean.setCurDate(AtguiguUtil.toDateTime(System.currentTimeMillis()));
                                out.collect(bean);
                            }
                        }
                );
    }

    private DataStream<TrafficPageViewBean> parseAndUnionOne(HashMap<String, DataStreamSource<String>> streams) {
        // 1. pv sv during_sum 的流
        SingleOutputStreamOperator<TrafficPageViewBean> pvSvDuringSumStream = streams.get(Constant.TOPIC_DWD_TRAFFIC_PAGE)
                .map(
                        json -> {
                            // 计算 pv sv during_sum
                            JSONObject obj = JSON.parseObject(json);
                            JSONObject common = obj.getJSONObject("common");
                            JSONObject page = obj.getJSONObject("page");

                            String vc = common.getString("vc");
                            String ch = common.getString("ch");
                            String ar = common.getString("ar");
                            String isNew = common.getString("is_new");

                            // String curDate = AtguiguUtil.toDate(System.currentTimeMillis());
                            Long uvCt = 0L;
                            Long svCt = page.getString("last_page_id") == null ? 1L : 0L;
                            Long pvCt = 1L;
                            Long durSum = page.getLong("during_time");
                            Long ujCt = 0L;
                            Long ts = obj.getLong("ts");

                            return new TrafficPageViewBean(
                                    "", "",  // 只有到开窗聚合之后才有窗口时间
                                    vc,
                                    ch,
                                    ar,
                                    isNew,
                                    "",  // 当天日期，等待聚合后，再添加也不迟
                                    uvCt,  // 以下为指标列
                                    svCt,
                                    pvCt,
                                    durSum,
                                    ujCt,
                                    ts
                            );
                        }
                );

        // 2. uv 的流
        SingleOutputStreamOperator<TrafficPageViewBean> uvStream = streams.get(Constant.TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL)
                .map(
                        json -> {
                            JSONObject obj = JSON.parseObject(json);
                            JSONObject common = obj.getJSONObject("common");
                            JSONObject page = obj.getJSONObject("page");

                            String vc = common.getString("vc");
                            String ch = common.getString("ch");
                            String ar = common.getString("ar");
                            String isNew = common.getString("is_new");

                            // String curDate = AtguiguUtil.toDate(System.currentTimeMillis());
                            Long uvCt = 1L;
                            Long svCt = 0L;
                            Long pvCt = 0L;
                            Long durSum = 0L;
                            Long ujCt = 0L;
                            Long ts = obj.getLong("ts");

                            return new TrafficPageViewBean(
                                    "", "",  // 只有到开窗聚合之后才有窗口时间
                                    vc,
                                    ch,
                                    ar,
                                    isNew,
                                    "",  // 当天日期，等待聚合后，再添加也不迟
                                    uvCt,  // 以下为指标列
                                    svCt,
                                    pvCt,
                                    durSum,
                                    ujCt,
                                    ts
                            );
                        }
                );

        // 3. uj 的流
        SingleOutputStreamOperator<TrafficPageViewBean> ujStream = streams.get(Constant.TOPIC_DWD_TRAFFIC_USER_JUMP_DETAIL)
                .map(
                        json -> {
                            JSONObject obj = JSON.parseObject(json);
                            JSONObject common = obj.getJSONObject("common");
                            JSONObject page = obj.getJSONObject("page");

                            String vc = common.getString("vc");
                            String ch = common.getString("ch");
                            String ar = common.getString("ar");
                            String isNew = common.getString("is_new");

                            // String curDate = AtguiguUtil.toDate(System.currentTimeMillis());
                            Long uvCt = 0L;
                            Long svCt = 0L;
                            Long pvCt = 0L;
                            Long durSum = 0L;
                            Long ujCt = 1L;
                            Long ts = obj.getLong("ts");

                            return new TrafficPageViewBean(
                                    "", "",  // 只有到开窗聚合之后才有窗口时间
                                    vc,
                                    ch,
                                    ar,
                                    isNew,
                                    "",  // 当天日期，等待聚合后，再添加也不迟
                                    uvCt,  // 以下为指标列
                                    svCt,
                                    pvCt,
                                    durSum,
                                    ujCt,
                                    ts
                            );
                        }
                );

        // 4. 三个流合并
        return pvSvDuringSumStream.union(uvStream, ujStream);
    }
}


/**
 * todo 报错待解决
 * 0    [doris-streamload-output-format-thread-1] ERROR org.apache.doris.flink.table.DorisDynamicOutputFormat  - doris sink error, retry times = 0
 * org.apache.doris.flink.exception.StreamLoadException: stream load error: errCode = 2, detailMessage = data cannot be inserted into table with empty partition. Use `SHOW PARTITIONS FROM dws_traffic_vc_ch_ar_is_new_page_view_window` to see the currently partitions of this table. , see more in null
 * 	at org.apache.doris.flink.table.DorisStreamLoad.load(DorisStreamLoad.java:109)
 * 	at org.apache.doris.flink.table.DorisDynamicOutputFormat.flush(DorisDynamicOutputFormat.java:319)
 * 	at org.apache.doris.flink.table.DorisDynamicOutputFormat.lambda$open$1(DorisDynamicOutputFormat.java:205)
 * 	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
 * 	at java.util.concurrent.FutureTask.runAndReset(FutureTask.java:308)
 * 	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:180)
 * 	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:294)
 * 	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
 * 	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
 * 	at java.lang.Thread.run(Thread.java:745)
 */
























