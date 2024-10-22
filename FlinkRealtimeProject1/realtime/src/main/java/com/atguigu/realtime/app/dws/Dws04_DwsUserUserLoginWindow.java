package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.UserLoginBean;
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

import java.time.Duration;

/**
 * @author: yuan.xin
 * @createTime: 2024/10/21 19:22
 * @contact: yuanxin9997@qq.com
 * @description: 10.4 用户域用户登陆各窗口汇总表
 * 10.4.1 主要任务
 * 从 Kafka 页面日志主题读取数据，统计七日回流用户和当日独立用户数预处理。
 * 10.4.2 思路分析
 * 之前的活跃用户，一段时间未活跃（流失），今日又活跃了，就称为回流用户。此处要求统计回流用户总数。规定当日登陆，且自上次登陆之后至少 7 日未登录的用户为回流用户。
 * 1）读取 Kafka 页面主题数据
 * 2）转换数据结构
 * 	流中数据由 String 转换为 JSONObject。
 * 3）过滤数据
 * 	统计的指标与用户有关，uid 不为 null 的数据才是有用的。此外，登陆分为两种情况：（1）用户打开应用后自动登录；（2）用户打开应用后没有登陆，浏览部分页面后跳转到登录页面，中途登陆。对于情况（1），登录操作发生在会话首页，所以保留首页即可；对于情况（2），登陆操作发生在 login 页面，login 页面之后必然会跳转到其它页面，保留 login 之后的页面即可记录情况（2）的登陆操作。
 * 综上，我们应保留 uid 不为 null 且 last_page_id 为 null 或 last_page_id 为 login 的浏览记录。
 * 4）设置水位线
 * 5）按照 uid 分组
 * 	不同用户的登陆记录互不相干，各自处理。
 * 6）统计回流用户数和独立用户数
 * 	运用 Flink 状态编程，记录用户末次登陆日期。
 * （1）若状态中的末次登陆日期不为 null，进一步判断。
 * ① 如果末次登陆日期不等于当天日期则独立用户数 uuCt 记为 1，并将状态中的末次登陆日期更新为当日，进一步判断。
 * a）如果当天日期与末次登陆日期之差大于等于 8 天则回流用户数 backCt 置为 1。
 * b）否则 backCt 置为 0。
 * ② 若末次登陆日期为当天，则 uuCt 和 backCt 均为 0，此时本条数据不会影响统计结果，舍弃，不再发往下游。
 * （2）如果状态中的末次登陆日期为 null，将 uuCt 置为 1，backCt 置为 0，并将状态中的末次登陆日期更新为当日。
 * 7）开窗，聚合
 * 度量字段求和，补充窗口起始和结束时间，时间戳字段置为当前系统时间，用于 ClickHouse 数据去重。
 * 8）写入 Doris
 *
 * 10.4.4 Doris建表语句
 * drop table if exists dws_user_user_login_window;
 * create table if not exists dws_user_user_login_window
 * (
 *     `stt`      DATETIME comment '窗口起始时间',
 *     `edt`      DATETIME comment '窗口结束时间',
 *     `cur_date` DATE comment '当天日期',
 *     `back_ct`  BIGINT replace comment '回流用户数',
 *     `uu_ct`    BIGINT replace comment '独立用户数'
 * ) engine = olap aggregate key (`stt`, `edt`, `cur_date`)
 * comment "用户域用户登陆各窗口汇总表"
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
public class Dws04_DwsUserUserLoginWindow extends BaseAppV1 {
    public static void main(String[] Args) {
        new Dws04_DwsUserUserLoginWindow().init(
                40004,
                2,
                "Dws04_DwsUserUserLoginWindow",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 先找到用户登录记录
        SingleOutputStreamOperator<JSONObject> loginLogStream = findLoginLog(stream);
        // loginLogStream.print();

        // 2. 找到当日登录用户记录 + 七日回流用户记录
        SingleOutputStreamOperator<UserLoginBean> uvAndReturnUserStream = findUVAndReturnUser(loginLogStream);// .print()

        // 3. 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultStream = windowAndAgg(uvAndReturnUserStream);// .print()

        // 4. 写入Doris
        writeToDoris(resultStream);

    }

    private SingleOutputStreamOperator<UserLoginBean> windowAndAgg(SingleOutputStreamOperator<UserLoginBean> uvAndReturnUserStream) {
        return uvAndReturnUserStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserLoginBean>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<UserLoginBean>() {
                            @Override
                            public UserLoginBean reduce(UserLoginBean bean1,
                                                        UserLoginBean bean2) throws Exception {
                                bean1.setUuCt(bean1.getUuCt() + bean2.getUuCt());
                                bean1.setBackCt(bean1.getBackCt() + bean2.getBackCt());
                                return bean1;
                            }
                        },
                        new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>.Context ctx,
                                                Iterable<UserLoginBean> elements,
                                                Collector<UserLoginBean> out) throws Exception {
                                UserLoginBean bean = elements.iterator().next();
                                bean.setStt(AtguiguUtil.toDateTime(ctx.window().getStart()));
                                bean.setEdt(AtguiguUtil.toDateTime(ctx.window().getEnd()));
                                bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
                                out.collect(bean);
                            }
                        }
                )
        ;
    }

    private SingleOutputStreamOperator<UserLoginBean> findUVAndReturnUser(SingleOutputStreamOperator<JSONObject> loginLogStream) {
        return loginLogStream
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"))
                // process里面是一个匿名内部类
                .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                    private ValueState<String> lastLoginDateState;  // 用户最后一次登录日期

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDateState", String.class));
                    }

                    /**
                     * 计算独立用户和回流用户
                     * todo 逻辑比较绕，需要仔细体会
                     * @param obj The input value.
                     * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
                     *     {@link TimerService} for registering timers and querying the time. The context is only
                     *     valid during the invocation of this method, do not store it.
                     * @param out The collector for returning result values.
                     * @throws Exception
                     */
                    @Override
                    public void processElement(JSONObject obj,
                                               KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx,
                                               Collector<UserLoginBean> out) throws Exception {
                        Long ts = obj.getLong("ts");
                        String thisLoginDate = AtguiguUtil.toDate(ts);
                        String lastLoginDate = lastLoginDateState.value();  // 最后一次登录日期
                        Long uuCt = 0L;
                        Long backCt = 0L;
                        // 如果这次的登录日期与最后一次的登录日期不相等，这次是今天的第一次登录
                        if (!thisLoginDate.equals(lastLoginDate)) {  // todo 不判断2个日期的大小吗？
                            // 今天的第一次登录
                            lastLoginDateState.update(thisLoginDate);  // 把状态更新为这次登录的日期
                            uuCt = 1L;
                            // 判断这个用户是否为回流用户
                            // 当最后一次登录日期不为空，表示这个用户不是新用户登录，才有必要判断是否为回流
                            if (lastLoginDate != null) {
                                Long lastLoginTs = AtguiguUtil.toTimeStamp(lastLoginDate);
                                Long thisLoginTs = AtguiguUtil.toTimeStamp(thisLoginDate);
                                if ((thisLoginTs - lastLoginTs) / (1000 * 60 * 60 * 24) > 7) {
                                    backCt = 1L;
                                }
                            }
                        }
                        if (uuCt == 1) {
                            out.collect(new UserLoginBean(
                                    "",
                                    "",
                                    "",
                                    backCt,
                                    uuCt,
                                    ts));
                        }
                    }
                })
                ;
    }

    /**
     * 查找登录日志
     *
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> findLoginLog(DataStreamSource<String> stream) {
        // 主动登录：
        //      用户主动打开登录页面，输入用户名和密码，实现登录
        //      用户在访问其他页面的过程中，当执行一些需要登录才能的操作的时候，页面会跳转到登录页面，然后完成登录
        //      当主动登录成功之后，会进入另外一个页面，这个页面的last_page_id is login，并且当前页面的uid is not null
        // 自动登录：
        //     一旦进入用户页面，当前页面根据你的cookie或缓存等，在后台自动实现登录
        //         找到这个用户当天第一条登录成功的记录
        //         什么记录才算？第一个包含uid字段的日志  uid is not null
        //
        return stream
                .map(JSON::parseObject)
                .filter(obj -> {
                    String uid = obj.getJSONObject("common").getString("uid");
                    String lastPageId = obj.getJSONObject("page").getString("last_page_id");
                    // 自动登录 uid != null && lastPageId == null
                    // 主动登录（登录成功之后的页面） uid != null && (lastPageId == "login" || lastPageId == "home")
                    return uid != null && (lastPageId == null || "login".equals(lastPageId) || "home".equals(lastPageId));
                })
                ;
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
                        "gmall2022.dws_user_user_login_window"
                ))
                ;

    }
}


/**
 * todo 报错
 * {"back_ct":0,"cur_date":"2024-10-22","edt":"2024-10-22 20:54:45","stt":"2024-10-22 20:54:40","ts":1732280079000,"uu_ct":14}
 * 1    [Map -> Sink: Unnamed (1/2)#1422] ERROR org.apache.doris.flink.table.DorisDynamicOutputFormat  - doris sink error, retry times = 0
 * org.apache.doris.flink.exception.StreamLoadException: stream load error: errCode = 2, detailMessage = data cannot be inserted into table with empty partition. Use `SHOW PARTITIONS FROM dws_user_user_login_window` to see the currently partitions of this table. , see more in null
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