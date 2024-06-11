package com.atguigu.chapter05_sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.chapter05_source.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/11 21:10
 * @contact: yuanxin9997@qq.com
 * @description: Flink Sink ElasticSearch 无界流
 */
public class Flink03_Redis_ES_sink_1 {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        SingleOutputStreamOperator<WaterSensor> result = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.parseLong(data[1]), Integer.parseInt(data[2]));
                })
                .keyBy(WaterSensor::getId)
                .sum("vc");
        // 写入到ES
        List<HttpHost> hosts = Arrays.asList(
                new HttpHost("hadoop102", 9200),
                new HttpHost("hadoop103", 9200),
                new HttpHost("hadoop104", 9200)
        );
        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<WaterSensor>(
                hosts,  // ES的连接主机
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element,  // 需要写入到ES的元素
                                        RuntimeContext ctx,  // 运行时上下文
                                        RequestIndexer indexer) {  // ES的索引
                        String msg = JSON.toJSONString(element);
                        // index 数据库
                        // type 表（6.x开始type只能有1个，6.x之前可以有多个，从7.x开始type被废弃）
                        // document 行
                        // field value 列
                        // # 查看ES集群信息
                        //get _cat/nodes
                        //# 查看当前所有索引
                        //get /_cat/indices
                        // # 查看具体索引的数据
                        //GET sensor/_search
                        IndexRequest indexRequest = Requests
                                .indexRequest("sensor")  // 要写入的index
                                .type("_doc")  // 要写入的type
                                .id(element.getId())  // 写入的document id，如果不指定id，会随机分配1个id，如果指定id重复，则覆盖更新
                                .source(msg, XContentType.JSON);
                        indexer.add(indexRequest);  // 添加到索引器
                    }
                }
        );
        result.addSink(waterSensorBuilder.build());


        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
