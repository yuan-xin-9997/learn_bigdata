package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/07 21:28
 * @contact: yuanxin9997@qq.com
 * @description:
 * 第9章 数仓开发之DWD层
 * DWD层设计要点：
 * （1）DWD层的设计依据是维度建模理论，该层存储维度模型的事实表。
 * （2）DWD层表名的命名规范为dwd_数据域_表名。
 *
 * 9.1.1 主要任务
 * 1）数据清洗（ETL）
 * 	数据传输过程中可能会出现部分数据丢失的情况，导致 JSON 数据结构不再完整，因此需要对脏数据进行过滤。
 * 2）新老访客状态标记修复
 * 	日志数据 common 字段下的 is_new 字段是用来标记新老访客状态的，1 表示新访客，0 表示老访客。前端埋点采集到的数据可靠
 * 	性无法保证，可能会出现老访客被标记为新访客的问题，因此需要对该标记进行修复。
 * 3）分流
 * 	本节将通过分流对日志数据进行拆分，生成五张事务事实表写入 Kafka。
 * 流量域页面浏览事务事实表
 * 流量域启动事务事实表
 * 流量域动作事务事实表
 * 流量域曝光事务事实表
 * 流量域错误事务事实表
 *
 */
public class Dwd01_DwdBaseLogApp extends BaseAppV1 {
    public static void main(String[] Args) {
        new Dwd01_DwdBaseLogApp().init(
                2000,
                2,
                "Dwd01_DwdBaseLogApp",
                Constant.TOPIC_ODS_LOG);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
        // 1. etl
        DataStream<JSONObject> etledStream =
                etl(stream)
                // .print()
                ;

        // 2. 纠正新老客户的标签
        validateNewOrOld(etledStream)
                .print()
        ;

        // 3. 分流

        // 4. 写入到Kafka中
    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(DataStream<JSONObject> etledStream) {
        return etledStream
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String firstVisitDate = this.firstVisitDateState.value();
                        Long ts = value.getLong("ts");
                        String today = AtguiguUtil.toDate(ts);
                        JSONObject common = value.getJSONObject("common");
                        // if (firstVisitDate == null) {
                        //     // 表示第一次访问
                        //     // 状态更新为今天
                        //     firstVisitDateState.update(today);
                        //     // 如果is_new是1，不用修复
                        //     // 如果is_new是0，不会出现
                        // }else{
                        //     // 状态有值
                        //     // 如果is_new是1，判断状态和今天是否相等
                        //     //    如果相同，is_new不用修复
                        //     //    如果不同，is_new修复为0
                        //     // 如果is_new是0，不用修复
                        //     if ("1".equals(common.getString("is_new"))) {
                        //         if (!today.equals(firstVisitDate)) {
                        //             common.put("is_new", "0");
                        //         }
                        //     }
                        // }

                        // is_new有可能有错误，需要修改
                        if ("1".equals(common.getString("is_nwe"))) {
                            // 状态是空，确实是第一次，不需要修改，但是需要更新状态
                            if (firstVisitDateState == null) {
                                firstVisitDateState.update(today);
                            } else if (!today.equals(firstVisitDate)) { // 状态不为空，是否和今天相等，如果不相等，需要更新
                                common.put("is_new", 0);
                            }
                        } else {
                            // is_new是0，状态是空，意味着，用户曾经访问过，但是程序中没有记录第一次访问时间
                            // 把状态更新成昨天
                            String yesterday = AtguiguUtil.toDate(ts - (24 * 60 * 60 * 1000));
                            firstVisitDateState.update(yesterday);
                        }
                        return value;
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(json -> {
                    try {
                        JSON.parseObject(json);
                    } catch (Exception e) {
                        System.out.println("JSON 格式有误，请检查: " + json);
                        return false;
                    }
                    return true;
                })
                .map(JSON::parseObject);

    }
}


/**
 * 1. 维度表数据
 * java -jar gmall2020-mock-db-2021
 * 只会产生user_info
 * bootstrap...
 * <p>
 * 2. yarn集群跑flink
 * per-job 过时
 * application 用这个
 * session
 * 先启动集群
 * 再提交job
 * <p>
 * ------
 * 纠正新老客户的标签 修复逻辑
 * 定义一个状态，存储年月日，用户第一次访问的年月日
 * is_new = 1
 * state和今天是同一天，或者状态中没有值   is_new不用修复
 * state和今天不是同一天                is_new修复为0
 * is_new = 0
 * 一定是老用户，不用修复
 * 更新状态：更新成为昨天
 */














