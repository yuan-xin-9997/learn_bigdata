package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
 */
public class Dws02_DwsTrafficVcChArIsNewPageViewWindow extends BaseAppV2 {
    public static void main(String[] Args) {
        new Dws02_DwsTrafficVcChArIsNewPageViewWindow().init(
                4002,
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
        streams.get(Constant.TOPIC_DWD_TRAFFIC_PAGE).print(Constant.TOPIC_DWD_TRAFFIC_PAGE);
        streams.get(Constant.TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL).print(Constant.TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL);
        streams.get(Constant.TOPIC_DWD_TRAFFIC_USER_JUMP_DETAIL).print(Constant.TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL);

        // 1. 把流转成同一种类型，然后union成一个流
        parseAndUnionOne(streams);

        // 2. 开窗聚合

        // 3. 写到doris中
    }

    private void parseAndUnionOne(HashMap<String, DataStreamSource<String>> streams) {
        streams.get(Constant.TOPIC_DWD_TRAFFIC_PAGE)
                .map(
                        json -> {
                            // 计算 pv sv during_sum
                            JSONObject obj = JSON.parseObject(json);
                            JSONObject common = obj.getJSONObject("common");
                            String vc = common.getString("vc");
                            String ch = common.getString("ch");
                            String ar = common.getString("ar");
                            String isNew = common.getString("is_new");

                            // String curDate = AtguiguUtil.toDate(System.currentTimeMillis());

                        }
                );
    }
}



























