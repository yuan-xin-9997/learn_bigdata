import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static java.lang.System.setProperty;

/**
 * @author: yuan.xin
 * @createTime: 2024年8月21日18:41:33
 * @contact: yuanxin9997@qq.com
 * @description: Flink SQL Join - 常规Join - 左连接 - 使用SQL方式消费left join数据
 * ref: https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/joins/
 *
 * Joins #
 * Batch Streaming
 *
 * Flink SQL supports complex and flexible join operations over dynamic tables. There are several different types of joins to account for the wide variety of semantics queries may require.
 *
 * By default, the order of joins is not optimized. Tables are joined in the order in which they are specified in the FROM clause. You can tweak the performance of your join queries, by listing the tables with the lowest update frequency first and the tables with the highest update frequency last. Make sure to specify tables in an order that does not yield a cross join (Cartesian product), which are not supported and would cause a query to fail.
 *
 * Regular Joins #
 * Regular joins are the most generic type of join in which any new record, or changes to either side of the join, are visible and affect the entirety of the join result. For example, if there is a new record on the left side, it will be joined with all the previous and future records on the right side when the product id equals.
 *
 *
 * 内连接：只有新增数据
        connector可以用kafka（建议用），也可以用upsert-kakfa
 * 左连接：会有更新
 *      connector只能用upsert-kafka
 */
public class Join_RegularJoin4 {
    public static void main(String[] Args) {
        // 设置环境变量
        setProperty("HADOOP_USER_NAME", "atguigu");

        // Web UI 端口设置
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20001);

        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置并行度，如果不设置，默认并行度=CPU核心数
        env.setParallelism(1);

        // Flink程序主逻辑
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);  // 创建表环境

        // 给Join的时候的状态设置ttl(要记得设置，否则状态会膨胀)
        // tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));  // 如果没有join操作10s，才会把左表状态清空

        // tEnv.executeSql("CREATE TABLE s3(id string, " +
        //         " name string, " +
        //         " age int, " +
        //         " primary key(id) not enforced " +
        //         ") WITH (" +
        //         " 'connector' = 'upsert-kafka'," +
        //         " 'properties.bootstrap.servers'='hadoop162:9092', " +
        //         " 'properties.group.id'='atguigu', " +
        //         " 'topic' = 's3', " +
        //         " 'key.format' = 'json', " +
        //         " 'value.format' = 'json'" +
        //         ")")
        //         ;
        // tEnv.sqlQuery("select * from s3").execute().print();

        tEnv.executeSql("CREATE TABLE s3(id string, " +
                " name string, " +
                " age int " +
                // " ,primary key(id) not enforced " +
                ") WITH (" +
                " 'connector' = 'kafka'," +
                " 'properties.bootstrap.servers'='hadoop162:9092', " +
                " 'properties.group.id'='abc', " +
                " 'topic' = 's3', " +
                " 'format' = 'json'" +
                ")")
                ;

        tEnv.sqlQuery("select * from s3").execute().print();


        // 懒加载
        // try {
        //     env.execute("a flink app");
        // } catch (Exception e) {
        //     throw new RuntimeException(e);
        // }

    }
}
