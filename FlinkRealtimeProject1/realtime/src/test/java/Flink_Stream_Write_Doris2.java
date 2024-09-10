import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;

import java.util.Properties;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024年9月10日20:16:25
 * @contact: yuanxin9997@qq.com
 * @description: Flink 通过 Stream 的方式 写入 Doris   (POJO)
 */
public class Flink_Stream_Write_Doris2 {
    public static void main(String[] Args) {
        // 设置环境变量
        setProperty("HADOOP_USER_NAME", "atguigu");

        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("strip_outer_array", "true");

        // Doris 流 只支持两种数据 sink : 1. json 字符串   2. RowData
        env
                .fromElements(
                        // "{\"siteid\": \"11\", \"citycode\": \"1002\", \"username\": \"abcd\",\"pv\": \"1000\"}"
                        new Site(12, (short) 10, "lisi", 2000L)
                )
                .map(ele -> JSON.toJSONString(ele))
                .addSink(DorisSink
                        .sink(
                        new DorisExecutionOptions.Builder()
                                .setBatchIntervalMs(2000L)
                                .setBatchSize(1024 * 1024)
                                .setEnableDelete(false)
                                .setMaxRetries(3)
                                .setStreamLoadProp(props)
                                .build(),
                        new DorisOptions.Builder()
                                .setFenodes("hadoop162:7030")
                                .setUsername("root")
                                .setPassword("aaaaaa")
                                .setTableIdentifier("test_db.table1")
                                .build()
                ))
        ;

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Site{
        private Integer siteId;
        private Short cityCode;
        private String username;
        private Long pv;
    }}


/*

todo 原因待排查
mysql> select * from table1;
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|   NULL |     NULL | lisi     | 2000 |
+--------+----------+----------+------+
1 row in set (0.03 sec)

mysql>

 */