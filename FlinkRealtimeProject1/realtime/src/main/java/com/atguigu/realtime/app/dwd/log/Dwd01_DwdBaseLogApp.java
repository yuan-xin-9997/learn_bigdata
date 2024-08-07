package com.atguigu.realtime.app.dwd.log;

import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/07 21:28
 * @contact: yuanxin9997@qq.com
 * @description:
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
    }
}