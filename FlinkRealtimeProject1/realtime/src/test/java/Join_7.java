import com.atguigu.realtime.app.BaseAppV1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.ZoneOffset;

import static java.lang.System.setProperty;
import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment;

/**
 * @author: yuan.xin
 * @createTime: 2024年8月28日19:19:45
 * @contact: yuanxin9997@qq.com
 * @description:
 */
public class Join_7 {
    public static void main(String[] args) {
        // 设置环境变量
        setProperty("HADOOP_USER_NAME", "atguigu");

        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(8));
        // tEnv.getConfig().getConfiguration().setString("table.local-time-zone", "UTC");
        // 给join的时候的状态设量ttL
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        tEnv.executeSql("create table t1(" +
                "id string, " +
                "name string," +
                "a as current_row_timestamp(), " +
                "b as date_format(current_row_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')" +
                ")with(" +
                "'connector'='kafka'," +
                "'properties.bootstrap.servers'='hadoop162:9092'," +
                "'properties.group.id'='atguigu'," +
                " 'topic'='s1'," +
                "'format'='csv'" +
                ")"
        );
        Table t1 = tEnv.from("t1");

        tEnv.toAppendStream(t1, Row.class).print();

        // 执行结果
        // +I[1, 1, 2024-08-28T11:29:14.458Z, 2024-08-28 19:29:14.458]

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
