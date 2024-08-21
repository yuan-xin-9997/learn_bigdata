import com.atguigu.realtime.app.BaseAppV1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024年8月21日18:41:33
 * @contact: yuanxin9997@qq.com
 * @description: Flink 使用流的方式消费Left Join的数据
 */
public class Join_RegularJoin5 extends BaseAppV1 {
    public static void main(String[] args) {
        new Join_RegularJoin5().init(
                10000,
                2,
                "Join_RegularJoin5",
                "s3"
        );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
