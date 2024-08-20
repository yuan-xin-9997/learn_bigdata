package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/20 19:37
 * @contact: yuanxin9997@qq.com
 * @description: 9.3 流量域用户跳出事务事实表
 *
 * 9.3.1 主要任务
 * 	过滤用户跳出明细数据。
 * 9.3.2 思路分析
 * 1）筛选策略
 * 跳出是指会话中只有一个页面的访问行为，如果能获取会话的所有页面，只要筛选页面数为 1 的会话即可获取跳出明细数据。
 * （1）离线数仓中我们可以获取一整天的数据，结合访问时间、page_id 和 last_page_id 字段对整体数据集做处理可以按照会话对页面日志进行划分，从而获得每个会话的页面数，只要筛选页面数为 1 的会话即可提取跳出明细数据；
 * （2）实时计算中无法考虑整体数据集，很难按照会话对页面访问记录进行划分。而本项目模拟生成的日志数据中没有 session_id（会话id）字段，也无法通过按照 session_id 分组的方式计算每个会话的页面数。
 * （3）因此，我们需要换一种解决思路。如果能判定首页日志之后没有同一会话的页面访问记录同样可以筛选跳出数据。如果日志数据完全有序，会话页面不存在交叉情况，则跳出页面的判定可以分为三种情况：① 两条紧邻的首页日志进入算子，可以判定第一条首页日志所属会话为跳出会话；② 第一条首页日志进入算子后，接收到的第二条日志为非首页日志，则第一条日志所属会话不是跳出会话；③ 第一条首页日志进入算子后，没有收到第二条日志，此时无法得出结论，必须继续等待。但是无休止地等待显然是不现实的。因此，人为设定超时时间，超时时间内没有第二条数据就判定为跳出行为，这是一种近似处理，存在误差，但若能结合业务场景设置合理的超时时间，误差是可以接受的。本程序为了便于测试，设置超时时间为 10s，为了更快看到效果可以设置更小的超时时间，生产环境的设置结合业务需求确定。
 * 由上述分析可知，情况 ① 的首页数据和情况 ③ 中的超时数据为跳出明细数据。
 * 2）知识储备
 * （1）Flink CEP
 * 跳出行为需要考虑会话中的两条页面日志数据（第一条为首页日志且超时时间内没有接收到第二条，或两条紧邻的首页日志到来可以判定第一条为跳出数据），要筛选的是组合事件，用 filter 无法实现这样的功能，由此引出 Flink CEP。
 * Flink CEP（Complex Event Processing 复杂事件处理）是在Flink上层实现的复杂事件处理库，可以在无界流中检测出特定的事件模型。用户定义复杂规则（Pattern），将其应用到流上，即可从流中提取满足 Pattern 的一个或多个简单事件构成的复杂事件。
 * （2）Flink CEP 定义的规则之间的连续策略
 * 严格连续: 期望所有匹配的事件严格的一个接一个出现，中间没有任何不匹配的事件。对应方法为 next()；
 * 松散连续: 忽略匹配的事件之间的不匹配的事件。对应方法为followedBy()；
 * 不确定的松散连续: 更进一步的松散连续，允许忽略掉一些匹配事件的附加匹配。对应方法为followedByAny()。
 * 3）实现步骤
 * （1）按照 mid 分组
 * 	不同访客的浏览记录互不干涉，跳出行为的分析应在相同 mid 下进行，首先按照 mid 分组。
 * （2）定义 CEP 匹配规则
 * ①规则一
 * 跳出行为对应的页面日志必然为某一会话的首页，因此第一个规则判定 last_page_id 是否为 null，是则返回 true，否则返回 false；
 * ②规则二
 * 规则二和规则一之间的策略采用严格连续，要求二者之间不能有其它事件。判断 last_page_id 是否为 null，在数据完整有序的前提下，如果不是 null 说明本条日志的页面不是首页，可以断定它与规则一匹配到的事件同属于一个会话，返回 false；如果是 null 则开启了一个新的会话，此时可以判定上一条页面日志所属会话为跳出会话，是我们需要的数据，返回 true；
 * ③超时时间
 * 超时时间内规则一被满足，未等到第二条数据则会被判定为超时数据。
 * （3）把匹配规则（Pattern）应用到流上
 * 	根据 Pattern 定义的规则对流中数据进行筛选。
 * （4）提取超时流
 * 	提取超时流，超时流中满足规则一的数据即为跳出明细数据，取出。
 * 	（5）合并主流和超时流，写入 Kafka 调出明细主题
 * （6）结果分析
 * 理论上 Flink 可以通过设置水位线保证数据严格有序（超时时间足够大），在此前提下，同一 mid 的会话之间不会出现交叉。若假设日志数据没有丢失，按照上述匹配规则，我们可以获得两类明细数据
 * ①两个规则都被满足，满足规则一的数据为跳出明细数据。在会话之间不会交叉且日志数据没有丢失的前提下，此时获取的跳出明细数据没有误差；
 * ②第一条数据满足规则二，超时时间内没有接收到第二条数据，水位线达到超时时间，第一条数据被发送到超时侧输出流。即便在会话之间不交叉且日志数据不丢失的前提下，此时获取的跳出明细数据仍有误差，因为超时时间之后会话可能并未结束，如果此时访客在同一会话内跳转到了其它页面，就会导致会话页面数大于 1 的访问被判定为跳出行为，下游计算的跳出率偏大。误差大小和设置的超时时间呈负相关关系，超时时间越大，理论上误差越小。
 *
 */

public class Dwd_03_DwdTrafficUserJumpDetail extends BaseAppV1 {
    public static void main(String[] Args) {
        new Dwd_03_DwdTrafficUserJumpDetail().init(
                3003,
                2,
                "Dwd03_DwdTrafficUserJumpDetail",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
                );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // stream.print();

        // 测试数据
        // stream = env
        //     .fromElements(
        //         "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
        //         "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
        //         "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
        //         "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
        //             "\"home\"},\"ts\":18000} ",
        //         "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
        //             "\"detail\"},\"ts\":30000} "
        //     );

        KeyedStream<JSONObject, String> keyedStream = stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> event.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));

        // 1. 定义CEP模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("entry1")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        // last_page_id 为空，说明是首页
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .next("entry2")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        // last_page_id 为空，说明是首页
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .next("second")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        // last_page_id 应该要不为空
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId != null || lastPageId.length() > 0;
                    }
                })
                .within(Time.seconds(5));

        // 2. 把模式作用到流中
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // 3. 从模式流中取出想要的数据
        SingleOutputStreamOperator<JSONObject> normal = patternStream
                .select(
                        new OutputTag<JSONObject>("timeout") {},  // 侧输出流名字
                        new PatternTimeoutFunction<JSONObject, JSONObject>() {  // 超时处理函数
                            @Override
                            public JSONObject timeout(Map<String, List<JSONObject>> map,
                                                      long timeoutTimestamp) throws Exception {
                                return map.get("entry1").get(0);
                            }
                        },
                        new PatternSelectFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                                return map.get("entry1").get(0);
                            }
                        }  // 正常匹配处理函数
                );

        // normal.getSideOutput(new OutputTag<JSONObject>("timeout") {}).print("timeout");
        // normal.print("normal");

        // 4. 把正常流和超时流合并，写入Kafka
        normal
                .union(normal.getSideOutput(new OutputTag<JSONObject>("timeout") {}))
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_USER_JUMP_DETAIL));
    }
}
