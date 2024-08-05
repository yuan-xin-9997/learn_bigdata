package com.atguigu.realtime.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024/07/31 20:05
 * @contact: yuanxin9997@qq.com
 * @description: flink实时数仓开发-ods层(Kafka) 到 dim层(HBase + Phoenix)
 *
 *
 * -------------------------------------------
 * 数仓分层规划：
 * 数仓分层
 *
 * ods
 *
 * 原始数据层
 * 存储位置: kafka
 *
 * topic:
 *     日志
 *         数据采集的数据到了kakfa, 其实就是ods层
 *
 *             ods_log
 *
 *
 *     业务数据
 *         maxwell
 *
 *             ods_db
 *
 * dim
 *    维度
 *
 *    存储位置:
 *       存入到支持sql的数据库中
 *         查询的频率比较高, 数据量还比较大
 *
 *         mysql: 可以选, 不是很好
 *
 *         hbase+Phoenix: 可以接入在线业务. 使用这个
 *             公司不是很方便
 *
 *         es:  支持 sql
 *             dsl 也可以直接读
 *
 *         redis: 主要在容器
 *
 *         hive: 不行
 *
 *    维度的用法:
 *
 *       假设有订单数据
 *         user_id   province_id
 *
 *         根据各种id查找对应的维度信息
 *
 *         user_1     province_1
 *
 *         随机查找
 *             select ...  from dim where id=?
 *
 *
 *
 *
 *
 *
 *
 * dwd
 * 来源于ods
 * 数据明细层
 *     日志: 启动 页面日志 曝光 活动 错误
 *
 *           分流:
 *                 每种日志写入到不同的topic中\
 *
 *                 用的流的方式
 *                     侧输出流
 *
 *     业务: 事实表 维度
 *       只管理事实表
 *
 *         对感兴趣的业务进行处理
 *             会对一些特别频繁使用的维度做退化,退化到事实表
 *
 *            使用sql技术
 *
 *            join ...
 *
 *
 * 存储位置?
 *     kafka, 给dws使用
 * dws
 * 数据汇总层
 *     轻度汇总
 *         小窗口(预聚合)
 *
 *     0-10
 *     10-20
 *
 *     存储到哪里?
 *         支持sql的数据
 *
 *         hbase+Phoenix 不选
 *
 *         clickhouse olap
 *
 *         drois olap
 *
 *
 *
 * ads
 *     最终的汇总
 *         非常的灵活
 *
 *     不做落盘, 根据前端的需求, 实时的汇总
 *
 *     select .. from t where time >=.. and time <= ... group by ...
 *
 */
public class Ods2Dim extends BaseAppV1 {

    public static void main(String[] Args) {
        new Ods2Dim().init(2001, 2, "Ods2Dim", Constant.TOPIC_ODS_DB);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 对流的操作
        // 1. 对业务数据做ETL
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        etledStream.print();
        // 2. 读取配置信息（MySQL）
        // 3. 数据流和广播流进行connect
        // 4. 根据不同的配置信息，把不同的维度写入到不同的Phoenix表中

    }

    /**
     * 过滤数据
     *
     * 注意：maxwell可使用bootstrap一次性全量读取mysql数据，并且以增量方式写入到kafka：
     *    bin/maxwell-bootstrap --config config.properties --database gmall2022 --table spu_info
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
         return stream
                .filter(json -> {
                    try {
                        // JSONObject obj = JSONObject.parseObject(json);
                        JSONObject obj = JSONObject.parseObject(json.replaceAll("bootstrap-", ""));
                        return "gmall2022".equals(obj.getString("database")) && (
                                "insert".equals(obj.getString("type")) ||
                                "update".equals(obj.getString("type"))
                                ) && obj.getString("data") != null
                                && obj.getString("data").length() > 2;
                    } catch (Exception e) {
                        System.out.println("json格式有误：" + json);
                        return false;
                    }
                })
                 //.map(json -> JSONObject.parseObject(json));  // 转成JSON Object 方便后续使用
                 .map(JSONObject::parseObject);  // 转成JSON Object 方便后续使用

    }
}
