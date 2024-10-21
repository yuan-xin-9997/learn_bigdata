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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

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
        findUVAndReturnUser(loginLogStream);

        // 3. 开窗聚合

        // 4. 写入Doris

    }

    private void findUVAndReturnUser(SingleOutputStreamOperator<JSONObject> loginLogStream) {
        loginLogStream
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
                                Long ts1 = AtguiguUtil.toTimeStamp(lastLoginDate);
                            }
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
                        "gmall2022."
                ))
                ;

    }
}
