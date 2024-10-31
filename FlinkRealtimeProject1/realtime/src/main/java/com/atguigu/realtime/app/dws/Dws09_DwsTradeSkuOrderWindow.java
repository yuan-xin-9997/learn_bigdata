package com.atguigu.realtime.app.dws;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TradeSkuOrderBean;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.AtguiguUtil;
import com.atguigu.realtime.util.DimUtil;
import com.atguigu.realtime.util.DruidDSUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;


/**
 * @author: yuan.xin
 * @createTime: 2024/10/30 20:17
 * @contact: yuanxin9997@qq.com
 * @description: 10.9 交易域SKU粒度下单各窗口汇总表
 *
 * 10.9 交易域SKU粒度下单各窗口汇总表
 * 10.9.1 主要任务
 * 从 Kafka 订单明细主题读取数据，按照唯一键对数据去重，分组开窗聚合，统计各维度各窗口的订单数、原始金额、活动减免金额、优惠券减免金额和订单金额，补全维度信息，将数据写入 Doris交易域SKU粒度下单各窗口汇总表。
 * 10.9.2 思路分析
 * 与上文提到的 DWS 层宽表相比，本程序新增了维度关联操作。
 * 维度表保存在 Hbase，首先要在 PhoenixUtil 工具类中补充查询方法。
 * 1）PhoenixUtil 查询方法思路
 * 本程序要通过已知的主键、表名从 HBase 中获取相关维度字段的值。根据已知信息，我们可以拼接查询语句，通过参数传递给查询方法，在方法内部执行注册驱动、获取连接对象、预编译（获取数据库操作对象）、执行、解析结果集、关闭资源六个步骤即可取出数据。
 * 查询结果必然要通过返回值的方式将数据传递给调用者。那么，返回值应该是什么类型？查询结果可能有多条，所以返回值应该是一个集合。确定了这一点，接下来要考虑集合元素用什么类型？查询结果可能有多个字段，此处提出两种方案：元组或实体类。下文将对两种方案的实现方式进行分析。
 * （1）元组
 * 如果用元组封装每一行的查询结果，可以有两种策略。a）把元组的元素个数传递给方法，然后通过 switch … case … 针对不同的元素个数调用对应的元组 API 对查询结果进行封装；b）把元组的 Class 对象传给方法，通过反射的方式将查询结果赋值给元组对象。a 的问题是需要编写大量的重复代码，对于每一个分支都要写一遍相同的处理逻辑；b 的问题是丢失了元组元素的类型信息。b 方案实现示例如下
 * public class TupleTest {
 *     public static void main(String[] args) throws InstantiationException, IllegalAccessException {
 *         Class<Tuple3> tuple3Class = Tuple3.class;
 *         Tuple3 tuple3 = tuple3Class.newInstance();
 *         Field[] declaredFields = tuple3Class.getDeclaredFields();
 *         for (int i = 1; i < declaredFields.length; i++) {
 *             Field declaredField = declaredFields[i];
 *             declaredField.setAccessible(true);
 *             declaredField.set(tuple3, (char)('a' + i));
 *         }
 *         System.out.println(tuple3);
 *     }
 * }
 * 结果如下
 * (b,c,d)
 * 	由于没有元组元素的类型信息，所以只能调用 Field 对象的 set 方法赋值，导致元组元素类型均为 Object，如此一来可能会为下游数据处理带来不便。
 * 	此外，Flink 提供的元组最大元素个数为 25，当查询结果字段过多时会出问题。
 *
 * 	（2）实体类
 * 	将实体类的 Class 对象通过参数传递到方法内，通过反射将查询结果赋值给实体类对象。
 * 	基于以上分析，此处选择自定义实体类作为集合元素，查询结果的每一行对应一个实体类对象，将所有对象封装到 List 集合中，返回给方法调用者。
 * 2）Phoenix 维度查询图解
 *
 * 3）旁路缓存优化
 * 外部数据源的查询常常是流式计算的性能瓶颈。以本程序为例，每次查询都要连接 Hbase，数据传输需要做序列化、反序列化，还有网络传输，严重影响时效性。可以通过旁路缓存对查询进行优化。
 * 旁路缓存模式是一种非常常见的按需分配缓存模式。所有请求优先访问缓存，若缓存命中，直接获得数据返回给请求者。如果未命中则查询数据库，获取结果后，将其返回并写入缓存以备后续请求使用。
 * （1）旁路缓存策略应注意两点
 * a）缓存要设过期时间，不然冷数据会常驻缓存，浪费资源。
 * b）要考虑维度数据是否会发生变化，如果发生变化要主动清除缓存。
 * （2）缓存的选型
 * 一般两种：堆缓存或者独立缓存服务（memcache，redis）
 * 堆缓存，性能更好，效率更高，因为数据访问路径更短。但是难于管理，其它进程无法维护缓存中的数据。
 * 独立缓存服务（redis,memcache），会有创建连接、网络IO等消耗，较堆缓存略差，但性能尚可。独立缓存服务便于维护和扩展，对于数据会发生变化且数据量很大的场景更加适用，此处选择独立缓存服务，将 redis 作为缓存介质。
 * （3）实现步骤
 * 从缓存中获取数据。
 * ① 如果查询结果不为 null，则返回结果。
 * ② 如果缓存中获取的结果为 null，则从 Phoenix 表中查询数据。
 * a）如果结果非空则将数据写入缓存后返回结果。
 * b）否则提示用户：没有对应的维度数据
 * 	注意：缓存中的数据要设置超时时间，本程序设置为 1 天。此外，如果原表数据发生变化，要删除对应缓存。为了实现此功能，需要对维度分流程序做如下修改：
 * 	i）在 MyBroadcastFunction的 processElement 方法内将操作类型字段添加到 JSON 对象中。
 * 	ii）在 DimUtil 工具类中添加 deleteCached 方法，用于删除变更数据的缓存信息。
 * 	iii）在 MyPhoenixSink 的 invoke 方法中补充对于操作类型的判断，如果操作类型为 update 则清除缓存。
 * 4）旁路缓存图解
 *
 * 5）异步 IO
 * 在Flink 流处理过程中，经常需要和外部系统进行交互，如通过维度表补全事实表中的维度字段。
 * 默认情况下，在Flink 算子中，单个并行子任务只能以同步方式与外部系统交互：将请求发送到外部存储，IO阻塞，等待请求返回，然后继续发送下一个请求。这种方式将大量时间耗费在了等待结果上。
 * 为了提高处理效率，可以有两种思路。
 * （1）增加算子的并行度，但需要耗费更多的资源。
 * （2）异步 IO。
 * Flink 在1.2中引入了Async I/O，将IO操作异步化。在异步模式下，单个并行子任务可以连续发送多个请求，按照返回的先后顺序对请求进行处理，发送请求后不需要阻塞式等待，省去了大量的等待时间，大幅提高了流处理效率。
 * Async I/O 是阿里巴巴贡献给社区的特性，呼声很高，可用于解决与外部系统交互时网络延迟成为系统瓶颈的问题。
 * 异步查询实际上是把维表的查询操作托管给单独的线程池完成，这样不会因为某一个查询造成阻塞，因此单个并行子任务可以连续发送多个请求，从而提高并发效率。对于涉及网络IO的操作，可以显著减少因为请求等待带来的性能损耗。
 * 6）异步 IO 图解
 *
 * 7）模板方法设计模式
 * 	（1）定义
 * 	在父类中定义完成某一个功能的核心算法骨架，具体的实现可以延迟到子类中完成。模板方法类一定是抽象类，里面有一套具体的实现流程（可以是抽象方法也可以是普通方法）。这些方法可能由上层模板继承而来。
 * （2）优点
 * 	在不改变父类核心算法骨架的前提下，每一个子类都可以有不同的实现。我们只需要关注具体方法的实现逻辑而不必在实现流程上分心。
 * 	本程序中定义了模板类 DimAsyncFunction，在其中定义了维度关联的具体流程
 * 	a）根据流中对象获取维度主键。
 * 	b）根据维度主键获取维度对象。
 * 	c）用上一步的查询结果补全流中对象的维度信息。
 * 8）去重思路分析
 * 我们在 DWD 层提到，订单明细表数据生成过程中会形成回撤流。left join 生成的数据集中，相同唯一键的数据可能会有多条。上文已有讲解，不再赘述。回撤数据在 Kafka 中以 null 值的形式存在，只需要简单判断即可过滤。我们需要考虑的是如何对其余数据去重。
 * 对回撤流数据生成过程进行分析，可以发现，字段内容完整数据的生成一定晚于不完整数据的生成，要确保统计结果的正确性，我们应保留字段内容最全的数据，基于以上论述，内容最全的数据生成时间最晚。要想通过时间筛选这部分数据，首先要获取数据生成时间。上文已经对FlinkSQL中几个获取当前时间戳的函数进行了讲解，此处不再赘述。获取时间之后要考虑如何比较时间，保留时间最大的数据，由此引出时间比较工具类。
 * （1）时间比较工具类
 * 	动态表中获取的数据生成时间精确到毫秒，前文提供的日期格式化工具类无法实现此类日期字符串向时间戳的转化，也就不能通过直接转化为时间戳的方式比较两条数据的生成时间。因此，单独封装工具类用于比较 TIME_STAMP(3) 类型的时间。比较逻辑是将时间拆分成两部分：小数点之前和小数点之后的。小数点之前的日期格式为 yyyy-MM-dd HH:mm:ss，这部分可以直接转化为时间戳比较，如果这部分时间相同，再比较小数点后面的部分，将小数点后面的部分转换为整型比较，从而实现 TIME_STAMP(3) 类型时间的比较。
 * （2）去重思路
 * 	获取了数据生成时间，接下来要考虑的问题就是如何获取生成时间最晚的数据。此处提供两种思路。
 * 	① 按照唯一键分组，开窗，在窗口闭合前比较窗口中所有数据的时间，将生成时间最晚的数据发送到下游，其它数据舍弃。
 * 	② 按照唯一键分组，对于每一个唯一键，维护状态和定时器，当状态中数据为 null 时注册定时器，把数据维护到状态中。此后每来一条数据都比较它与状态中数据的生成时间，状态中只保留生成最晚的数据。如果两条数据生成时间相同（系统时间精度不足），则保留后进入算子的数据。因为我们的 Flink 程序并行度和 Kafka 分区数相同，可以保证数据有序，后来的数据就是最新的数据。
 * 	两种方案都可行，此处选择方案二。
 * 	本节数据来源于 Kafka dwd_trade_order_detail 主题，后者的数据来源于 Kafka dwd_trade_order_pre_process 主题，dwd_trade_order_pre_process 数据生成过程中使用了 left join，因此包含 null 数据和重复数据。订单明细表读取数据使用的 Kafka Connector 会过滤掉 null 数据，程序内只做了过滤没有去重，因此该表不存在 null 数据，但对于相同唯一键 order_detail_id 存在重复数据。综上，订单明细表存在唯一键 order_detail_id 相同的数据，但不存在 null 数据，因此仅须去重。
 * 9）执行步骤
 * 	（1）从 Kafka 订单明细主题读取数据
 * 	（2）转换数据结构
 * 	（3）按照唯一键去重
 * 	（4）转换数据结构
 * 	JSONObject 转换为实体类 TradeSkuOrderBean。
 * （5）设置水位线
 * （6）分组、开窗、聚合
 * 按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间、结束时间和当天日期字段。
 * （7）维度关联，补充维度字段
 * ① 关联 sku_info 表
 * 	获取 sku_name，tm_id，category3_id，spu_id。
 * ② 关联 spu_info 表
 * 	获取 spu_name。
 * 	③ 关联 base_trademark 表
 * 	获取 tm_name。
 * 	④ 关联 base_category3 表
 * 	获取 name（三级品类名称），获取 category2_id。
 * 	⑤ 关联 base_categroy2 表
 * 	获取 name（二级品类名称），category1_id。
 * 	⑥ 关联 base_category1 表
 * 	获取 name（一级品类名称）。
 * （8）写出到 Doris。
 */
public class Dws09_DwsTradeSkuOrderWindow extends BaseAppV1 {
    public static void main(String[] Args) {
        new Dws09_DwsTradeSkuOrderWindow().init(
                40009,
                2,
                "Dws09_DwsTradeSkuOrderWindow",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 按照order_detail_id去重
        SingleOutputStreamOperator<JSONObject> distractedStream = distinctByOrderDetailId(stream);// .print()

        // 2. 封装数据到POJO中
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(distractedStream);

        // 2. 按照sku_id 分组 开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDim = windowAndAgg(beanStream);

        // 3. 补充维度信息
        addDim(beanStreamWithoutDim);

        // 4. 写出到Doris

    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> addDim(SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDim) {
        // 补充维度信息
        // 每来一条数据，需要查询6张维度表
        return beanStreamWithoutDim
                .map(
                        new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

                            private DruidDataSource druidDataSource;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                druidDataSource = DruidDSUtil.getDruidDataSource();
                            }

                            @Override
                            public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
                                DruidPooledConnection phoenixConn = druidDataSource.getConnection();
                                // 1. 查找dim_sku_info
                                // select * from dim_sku_info where id='1';
                                // JSONObject : {"列名": 值, ...}
                                JSONObject skuInfo = DimUtil.readDimFromPhoenix(phoenixConn, "dim_sku_info", bean.getSkuId());
                                bean.setSkuName(skuInfo.getString("SKU_NAME"));
                                return null;
                            }
                        }
                )
                ;
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAgg(SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean bean1,
                                                            TradeSkuOrderBean bean2) throws Exception {
                                bean1.setOriginalAmount(bean1.getOriginalAmount().add(bean2.getOriginalAmount()));
                                bean1.setActivityAmount(bean1.getActivityAmount().add(bean2.getActivityAmount()));
                                bean1.setCouponAmount(bean1.getCouponAmount().add(bean2.getCouponAmount()));
                                bean1.setOrderAmount(bean1.getOrderAmount().add(bean2.getOrderAmount()));
                                return bean1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s,
                                                ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean,
                                                        String, TimeWindow>.Context context,
                                                java.lang.Iterable<TradeSkuOrderBean> elements,
                                                Collector<TradeSkuOrderBean> out) throws Exception {
                                TradeSkuOrderBean bean = elements.iterator().next();
                                bean.setStt(AtguiguUtil.toDateTime(context.window().getStart()));
                                bean.setEdt(AtguiguUtil.toDateTime(context.window().getEnd()));
                                bean.setCurDate(AtguiguUtil.toDate(System.currentTimeMillis()));
                                out.collect(bean);
                            }
                        }
                )
                ;

    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(SingleOutputStreamOperator<JSONObject> distractedStream) {
        return distractedStream
                .map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject value) throws Exception {
                        // return new TradeSkuOrderBean()
                        // Builder 模式来构造对象，比较方便(Java lombok提供的trick)
                        return TradeSkuOrderBean.builder()
                                .skuId(value.getString("sku_id"))
                                .originalAmount(
                                        value.getBigDecimal("original_amount")==null ? new BigDecimal(0) : value.getBigDecimal("original_amount")
                                )
                                .orderAmount(value.getBigDecimal("order_amount")==null ? new BigDecimal(0) : value.getBigDecimal("order_amount"))
                                .activityAmount(value.getBigDecimal("activity_amount")==null ? new BigDecimal(0) : value.getBigDecimal("activity_amount"))
                                .couponAmount(value.getBigDecimal("coupon_amount")==null ? new BigDecimal(0) : value.getBigDecimal("coupon_amount"))
                                .ts(value.getLong("ts") * 1000)
                                .build();
                    }
                })
                ;
    }

    private SingleOutputStreamOperator<JSONObject> distinctByOrderDetailId(DataStreamSource<String> stream) {
        // 选择定时器
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> maxTsState;  // 状态 保存ts最大的数据

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        maxTsState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("maxTsState", JSONObject.class));
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 定时器触发的时候，把状态中的数据输出
                        out.collect(maxTsState.value());
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        // 第一条数据来的时候，把数据保存到状态，注册5秒后触发的定时器
                        if (maxTsState.value() == null) {
                            maxTsState.update(value);
                            ctx.timerService().registerEventTimeTimer(
                                    ctx.timerService().currentProcessingTime() + 5000L
                            );
                        }
                        // 如果不是第一条，则与状态中的数据对比
                        else {
                            String last = maxTsState.value().getString("row_op_ts");
                            String current = value.getString("row_op_ts");
                            if(AtguiguUtil.isLarger(current, last)){
                                maxTsState.update(value);
                            }
                        }
                    }
                })
                ;
    }
}






































