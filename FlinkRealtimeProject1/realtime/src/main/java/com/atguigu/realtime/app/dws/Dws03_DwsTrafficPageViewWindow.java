package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.realtime.bean.TrafficPageViewBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.time.Duration;

/**
 * @author: yuan.xin
 * @createTime: 2024/10/17 18:44
 * @contact: yuanxin9997@qq.com
 * @description: 10.3 流量域页面浏览各窗口汇总表
 *
 * 10.3.1 主要任务
 * 从 Kafka 页面日志主题读取数据，统计当日的首页和商品详情页独立访客数。
 * 10.3.2 思路分析
 * 1）读取 Kafka 页面主题数据
 * 2）转换数据结构
 * 将流中数据由 String 转换为 JSONObject。
 * 3）过滤数据
 * 	仅保留 page_id 为 home 或 good_detail 的数据，因为本程序统计的度量仅与这两个页面有关，其它数据无用。
 * 4）设置水位线
 * 5）按照 mid 分组
 * 6）统计首页和商品详情页独立访客数，转换数据结构
 * 运用 Flink 状态编程，为每个 mid 维护首页和商品详情页末次访问日期。如果 page_id 为 home，当状态中存储的日期为 null 或不是当日时，将 homeUvCt（首页独立访客数） 置为 1，并将状态中的日期更新为当日。否则置为 0，不做操作。商品详情页独立访客的统计同理。当 homeUvCt 和 detailUvCt 至少有一个不为 0 时，将统计结果和相关维度信息封装到定义的实体类中，发送到下游，否则舍弃数据。
 * 7）开窗
 * 8）聚合
 * 9）将数据写出到 Doris
 * 10.3.3 图解
 *
 * 10.3.4 Doris建表语句
 * drop table if exists dws_traffic_page_view_window;
 * create table if not exists dws_traffic_page_view_window
 * (
 *     `stt`               DATETIME comment '窗口起始时间',
 *     `edt`               DATETIME comment '窗口结束时间',
 *     `cur_date`          DATE comment '当天日期',
 *     `home_uv_ct`        BIGINT replace comment '首页独立访客数',
 *     `good_detail_uv_ct` BIGINT replace comment '商品详情页独立访客数'
 * ) engine = olap aggregate key (`stt`, `edt`, `cur_date`)
 * comment "流量域页面浏览各窗口汇总表"
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
 *
 *
 *
 *
 * 从kafka页面日志主题读取数据，统计当日的首页和商品详情页独立访客数
 * 1. 数据源
 *      页面日志
 * 2. 过滤出所有首页访问记录和详情页访问记录
 * 3. 封装bean中：如果首页  首页=1，如果是详情页  详情页=1
 * 4.  开窗聚合
 * 5. 写入到Doris
 */
public class Dws03_DwsTrafficPageViewWindow extends BaseAppV1 {
    public static void main(String[] Args) {
        new Dws03_DwsTrafficPageViewWindow().init(
                40003,
                2,
                "Dws03_DwsTrafficPageViewWindow",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 先找到详情页和首页，封装到bean中
        SingleOutputStreamOperator beanStream = findUv(stream);
        // beanStream.print();

        // 2. 开窗聚合
        SingleOutputStreamOperator windowAndAgg = windowAndAgg(beanStream);
        // windowAndAgg.print();

        // 3. 写入到Doris
        writeToDoris(windowAndAgg);
    }

    private void writeToDoris(SingleOutputStreamOperator resultStream) {
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
                        "gmall2022.dws_traffic_page_view_window"
                ))
                ;

    }

    private SingleOutputStreamOperator windowAndAgg(SingleOutputStreamOperator beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean bean1,
                                                                        TrafficHomeDetailPageViewBean bean2) throws Exception {
                                bean1.setHomeUvCt(bean1.getHomeUvCt() + bean2.getHomeUvCt());
                                bean1.setGoodDetailUvCt(bean1.getGoodDetailUvCt() + bean2.getGoodDetailUvCt());
                                return bean1;
                            }
                        },
                        new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context ctx,
                                                java.lang.Iterable<TrafficHomeDetailPageViewBean> elements,
                                                Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                                TrafficHomeDetailPageViewBean bean = elements.iterator().next();
                                bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                                bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
                                bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
                                out.collect(bean);
                            }
                        }
                )
                ;

    }

    private SingleOutputStreamOperator findUv(DataStreamSource<String> stream) {
        return stream
                .map(json-> JSON.parseObject(json))
                .filter(obj -> {
                    String pageId = obj.getJSONObject("page").getString("page_id");
                    return "home".equals(pageId) || "good_detail".equals(pageId);
                })
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))  // 统计home的独立访客数，需要把同一用户的记录分到同一组
                .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

                    private ValueState<String> goodDetailState;
                    private ValueState<String> homeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        homeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("homeState", String.class));
                        goodDetailState = getRuntimeContext().getState(new ValueStateDescriptor<String>("goodDetailState", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx,
                                               Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String pageId = obj.getJSONObject("page").getString("page_id");
                        Long ts = obj.getLong("ts");
                        // 找到这个用户今天第一个home页面记录
                        // 找到这个用户今天第一个good_detail页面记录
                        Long homeUvCt = 0L;
                        Long goodDetailUvCt = 0L;
                        String today = AtguiguUtil.toDate(ts);
                        // 是home，并且今天和状态中的年月日不相等，就是今天的第一个home
                        if ("home".equals(pageId) && !today.equals(homeState.value())) {
                            homeUvCt = 1L;
                            // 更新状态
                            homeState.update(today);
                        }else if("good_detail".equals(pageId) && !today.equals(goodDetailState.value())){
                            goodDetailUvCt = 1L;
                            goodDetailState.update(today);
                        }
                        // homeUvCt和goodDetailUvCt至少有1个不为1,
                        if(homeUvCt + goodDetailUvCt >= 1){
                            out.collect(new TrafficHomeDetailPageViewBean(
                                    "",
                                    "",
                                    "",
                                    homeUvCt,
                                    goodDetailUvCt,
                                    ts
                            ));
                        }

                    }
                })
      //          .print()
        ;
    }



}


/**
 * todo 报错
 * {"cur_date":"2024-10-17","edt":"2024-10-10 10:58:45","good_detail_uv_ct":1,"home_uv_ct":2,"stt":"2024-10-10 10:58:40","ts":1728529121000}
 * 1    [Map -> Sink: Unnamed (2/2)#13] ERROR org.apache.doris.flink.table.DorisDynamicOutputFormat  - doris sink error, retry times = 0
 * org.apache.doris.flink.exception.StreamLoadException: stream load error: errCode = 2, detailMessage = data cannot be inserted into table with empty partition. Use `SHOW PARTITIONS FROM dws_traffic_page_view_window` to see the currently partitions of this table. , see more in null
 *
 * A： 由于动态分区的特性
 * 当前时间是 2024年10月17日21:01:52
 * 插入的数据是
 * cur_date":"2024-10-17","edt":"2024-10-10 11:46:50","good_detail_uv_ct":3,"home_uv_ct":3,"stt":"2024-10-10 11:46:45"}
 * {"cur_date":"2024-10-17","edt":"2024-10-10 11:46:45","good_detail_uv_ct":2,"home_uv_ct":3,"stt":"2024-10-10 11:46:40"}
 * 在Doris中
 * dynamic_partition.start	动态分区的起始偏移，为负数。根据 time_unit 属性的不同，以当天（星期/月）为基准，分区范围在此偏移之前的分区将会被删除。如果不填写默认值为Interger.Min_VALUE 即-2147483648，即不删除历史分区
 * dynamic_partition.end	动态分区的结束偏移，为正数。根据 time_unit 属性的不同，以当天（星期/月）为基准，提前创建对应范围的分区
 * 解决办法：
 * 1. 修改数据库建表语句中关于动态分区的部分  （待进一步
 * 2. 修改生成的数据，日期为今天
 */
































