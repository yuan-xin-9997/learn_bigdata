package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.CartAddUuBean;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

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
                        obj.getLong("ts");
                    }
                })
    }
}
