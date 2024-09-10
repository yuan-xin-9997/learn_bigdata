import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Properties;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024/09/10 19:10
 * @contact: yuanxin9997@qq.com
 * @description: Flink 通过 Stream 的方式 读取 Doris
 */
public class Flink_Stream_Read_Doris {
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
        props.setProperty("fenodes", "hadoop162:7030");
        props.setProperty("username", "root");
        props.setProperty("password", "aaaaaa");
        props.setProperty("table.identifier", "test_db.table1");
        DorisStreamOptions opts = new DorisStreamOptions(props);

        env.addSource(new DorisSourceFunction(opts, new SimpleListDeserializationSchema()))
                .print();


        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}


/*
如果报错
Exception in thread "main" java.lang.NoSuchMethodError: org.apache.http.entity.StringEntity.<init>(Ljava/lang/String;Ljava/nio/charset/Charset;)V
	at org.apache.doris.flink.rest.RestService.findPartitions(RestService.java:496)
	at org.apache.doris.flink.datastream.DorisSourceFunction.<init>(DorisSourceFunction.java:58)
	at Flink_Stream_Read_Doris.main(Flink_Stream_Read_Doris.java:43)

	<!-- https://mvnrepository.com/artifact/org.apache.httpcomponents/httpcore -->
<dependency>
    <groupId>org.apache.httpcomponents</groupId>
    <artifactId>httpcore</artifactId>
    <version>4.3.2</version>
</dependency>

Exception in thread "main" java.lang.NoSuchMethodError: org.apache.http.client.methods.HttpRequestBase.setConfig(Lorg/apache/http/client/config/RequestConfig;)V
	at org.apache.doris.flink.rest.RestService.send(RestService.java:114)
	at org.apache.doris.flink.rest.RestService.findPartitions(RestService.java:501)
	at org.apache.doris.flink.datastream.DorisSourceFunction.<init>(DorisSourceFunction.java:58)
	at Flink_Stream_Read_Doris.main(Flink_Stream_Read_Doris.java:43)

<!-- https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient -->
<dependency>
    <groupId>org.apache.httpcomponents</groupId>
    <artifactId>httpclient</artifactId>
    <version>4.3.6</version>
</dependency>


 */
