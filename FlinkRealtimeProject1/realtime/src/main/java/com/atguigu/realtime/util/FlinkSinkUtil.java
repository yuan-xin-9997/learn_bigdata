package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/06 21:21
 * @contact: yuanxin9997@qq.com Flink Sink 工具类
 * @description:
 */
public class FlinkSinkUtil {
    public static void main(String[] Args) {}

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();
    }
}
