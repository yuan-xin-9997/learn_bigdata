package com.atguigu.chapter06;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024/06/18 19:28
 * @contact: yuanxin9997@qq.com
 * @description: 6.3各省份页面广告点击量实时统计
 * 电商网站的市场营销商业指标中，除了自身的APP推广，还会考虑到页面上的广告投放（包括自己经营的产品和其它网站的广告）。所以广告相关的统计分析，也是市场营销的重要指标。
 * 对于广告的统计，最简单也最重要的就是页面广告的点击量，网站往往需要根据广告点击量来制定定价策略和调整推广方式，而且也可以借此收集用户的偏好信息。更加具体的应用是，我们可以根据用户的地理位置进行划分，从而总结出不同省份用户对不同广告的偏好，这样更有助于广告的精准投放。
 * 数据准备
 * 在咱们当前的案例中，给大家准备了某电商网站的广告点击日志数据AdClickLog.csv,	本日志数据文件中包含了某电商网站一天用户点击广告行为的事件流，数据集的每一行表示一条用户广告点击行为，由用户ID、广告ID、省份、城市和时间戳组成并以逗号分隔。
 * 将文件放置项目目录: input下
 */
public class Flink04_Project_AdProvince {
    public static void main(String[] Args) {
        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        env.readTextFile("D:\\dev\\learn_bigdata\\FlinkDemo1\\input\\AdClickLog.csv")
                .map(line-> {
                    String[] datas = line.split(",");
                    return new AdsClickLog(Long.parseLong(datas[0]),
                            Long.parseLong(datas[1]),
                            datas[2],
                            datas[3],
                            Long.parseLong(datas[4]));
                })
                .map(new MapFunction<AdsClickLog, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(AdsClickLog value) throws Exception {
                        return Tuple2.of(value.getProvince() + "_" + value.getAdId(), 1L);
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        // 懒加载
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
