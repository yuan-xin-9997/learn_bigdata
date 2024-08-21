import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024/08/20 21:06
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL Join - 时态Join - Lookup Join
 * ref: https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/joins/
 *
 * Lookup Join #
 * A lookup join is typically used to enrich a table with data that is queried from an external system. The join requires one table to have a processing time attribute and the other table to be backed by a lookup source connector.
 *
 * The lookup join uses the above Processing Time Temporal Join syntax with the right table to be backed by a lookup source connector.
 *
 * The following example shows the syntax to specify a lookup join.
 *
 * Lookup Join(Flink SQL实现 事实表join维度表)
 * 	Lookup Join 通常在 Flink SQL 表和外部系统查询结果关联时使用。这种关联要求一张表（主表）有处理时间字段，而另一张表（维表）由 Lookup 连接器生成。
 * 	Lookup Join 做的是维度关联，而维度数据是有时效性的，那么我们就需要一个时间字段来对数据的版本进行标识。因此，Flink 要求我们提供处理时间用作版本字段。
 * 	此处选择调用 PROCTIME() 函数获取系统时间，将其作为处理时间字段。该函数调用示例如下
 * tableEnv.sqlQuery("select PROCTIME() proc_time")
 *                 .execute()
 *                 .print();
 * // 结果
 * +----+-------------------------+
 * | op |               proc_time |
 * +----+-------------------------+
 * | +I | 2022-04-09 15:45:50.752 |
 * +----+-------------------------+
 * 1 row in set
 *
 * （4）JDBC SQL Connector 参数解读
 * connector：连接器类型，此处为 jdbc
 * url：数据库 url
 * table-name：数据库中表名
 * lookup.cache.max-rows：lookup 缓存中的最大记录条数
 * lookup.cache.ttl：lookup 缓存中每条记录的最大存活时间
 * username：访问数据库的用户名
 * password：访问数据库的密码
 * driver：数据库驱动，注意：通常注册驱动可以省略，但是自动获取的驱动是 com.mysql.jdbc.Driver，Flink CDC 2.1.0 要求 mysql 驱动版本必须为 8 及以上，在 mysql-connector -8.x 中该驱动已过时，新的驱动为 com.mysql.cj.jdbc.Driver。省略该参数控制台打印的警告如下
 * Loading class `com.mysql.jdbc.Driver'. This is deprecated. The new driver class is `com.mysql.cj.jdbc.Driver'. The driver is automatically registered via the SPI and manual loading of the driver class is generally unnecessary.
 *
 */
public class Join_LookupJoin6 {
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
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);  // 创建表环境

        // 此参数对Lookup Join的两边的两张表都无效
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        tEnv.executeSql("CREATE TABLE t1(" +
                "  id string, " +
                // "  name string, " +
                "  pt as proctime() " +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'properties.bootstrap.servers'='hadoop162:9092', " +
                " 'properties.group.id'='atguigu', " +
                " 'topic' = 's1'," +
                " 'format' = 'csv'" +
                ")")
        ;

        // Lookup Join默认不会给下表添加缓存
        tEnv.executeSql("CREATE TABLE dic(" +
                "  dic_code string, " +
                "  dic_name string " +
                ") WITH (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:mysql://hadoop162:3306/gmall2022?useSSL=false', " +
                " 'table-name' = 'base_dic', " +
                " 'username' = 'root', " +
                " 'password' = 'aaaaaa'," +
                " 'lookup.cache.max-rows'='10', " +
                " 'lookup.cache.ttl'='30 second' " +
                ")")
        ;

        tEnv.sqlQuery("select " +
                "id, " +
                "d.dic_name " +
                " from t1 " +
                " join dic FOR SYSTEM_TIME AS OF t1.pt as d " +
                " on t1.id = d.dic_code ")
                .execute()
                .print();

        // 懒加载
        try {
            env.execute("a flink app");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
