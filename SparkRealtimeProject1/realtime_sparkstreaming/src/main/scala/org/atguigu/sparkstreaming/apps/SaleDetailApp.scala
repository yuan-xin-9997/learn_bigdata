package org.atguigu.sparkstreaming.apps

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.constants.{DBNameConstant, PrefixConstant, TopicConstant}
import com.atguigu.realtime.utils.{PropertiesUtil, RedisUtil}
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.atguigu.sparkstreaming.beans.{ActionLog, CouponAlertInfo, OrderDetail, OrderInfo, ProvinceInfo, SaleDetail, UserInfo}
import org.atguigu.sparkstreaming.utils.{DStreamUtil, DataParseUtil}
import redis.clients.jedis.Jedis

import java.time.LocalDate
import java.util
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 15:56
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 *
 *               Spark实时项目：购物明细需求
 *               at least once + 幂等输出  保证精确计算一次
 *               采用ES存储运算结果，在SparkConf中需要添加相关参数
 * ----------------------------------------
 * 购物明细：
 *    order_detail
 *      left join order_info on order_detail.order_id=order_info.id
 *        目的：从order_info获得当前这笔订单详情的province_id和user_id
 *        使用 province_id和user_id 到对应的维度表去关联维度信息即可
 *
 * 原则：事实表的关联，只能在流Stream中操作。原因是事实表中的事实是源源不断地产生
 * 关联维度表时候，区分关联维度的类型：
 *    1.几乎不变的维度表（不会insert、也不会update）：时间、日历、省份地区、
 *    2.只会新增的记录的维度表：比如sku_info，当商家上架了新的商品时，会向表中新增商品。当商品的的信息一旦入库，信息禁止修改
 *    3.会变化的维度表（即会有insert还有update）：用户表usert_info
 *
 *    对于第1种维度表，不用监控，在用的时候，直接去mysql查询即可，查询一次后，直接在App端缓存即可
 *    对于2、3种维度表，需要实时监控变化
 *
 * -----------------------------------------
 * 1. 在实时的流中消费order_detail和order_info，进行join
 * 2. 监控实时insert和update的用户信息（维度表），将信息从MySQL同步到Redis
 *    join用户信息时，只需要查询Redis即可
 * 3. 不变的维度，可以在App运行之前，一次性从MySQL查询，缓存到App端
 *    广播变量（分布式缓存）
 *    将从Driver端广播的数据分发到Executor端进行缓存。在每个Executor运行的Task都可以从Executor端读取数据
 * -----------------------------------------
 * 实时的流中消费order_detail和order_info，进行join的条件：
 *  1. 两个流必须从同一个StreamingContext获取
 *  2. 只有DStream[K,V]类型的才能join
 *     order_detail
 *          left join order_info on order_detail.order_id=order_info.id
 *          把on中的字段作为K
 * -----------------------------------------
 * 无法成功Join的根本原因：要Join的数据无法在同一个批次被消费到。
 * 数量关系：1个order_info对应N个order_detail
 * 解决原则：早消费的数据写入到Redis缓存，晚消费到的数据，到缓存中进行关联
 * 对于每一个批次的order_info，处理逻辑：
 *    1. 和当前批次已消费到的order_detail关联
 *    2. 到Redis缓存中去找之前已经早消费到order_detail，缓存里面有就关联
 *    3. 将order_info写入到缓存，以防止后续有晚到的order_detail
 * 对于每一个批次的order_detail，处理逻辑：
 *    4. 和当前批次已消费到的order_info关联，如果order_info做过关联操作，此步骤可以省略
 *    5. 无法关联的order_detail需要到Redis缓存中去找之前已经早消费到order_info，缓存里面有就关联
 *    6. 如果5.中缓存无对应的order_info，则将order_detail写入到缓存，以防止后续有晚到的order_info
 * -----------------------------------------
 * Redis缓存中order_info和order_detail如何存储？
 *       当前的需求是，对某个早到或晚到order_detail，根据order_id求订单信息
 *          key:  orderinfo:order_id
 *          value:  string
 *       要存储的数据:Order_Detail
 *          当前的需求是，对某个早到或晚到order_info，根据order_id求其所有的订单详情信息，由于1笔order_info对应N笔order_detail，故使用Set集合存储。
 *          key:  orderdetail:order_id
 *          value:  set
 * -----------------------------------------
 * todo 报错 java.lang.NullPointerException
 * 未排查到原因
 */
object SaleDetailApp extends BaseApp {

  // 重写消费者组、要消费的topic名
  override var groupId: String = "SaleDetailApp"
  override var topic: String = TopicConstant.ORDER_DETAIL
  var topic2: String = TopicConstant.ORDER_INFO
  // 重写SparkStreaming App 名、采集时间周期
  override var appName: String = "SaleDetailApp"
  override var batchDuration: Int = 10

  /**
   * 从MySQL中查询省份数据
   * 方式1: JDBC方式  获取连接，准备SQL，预编译，查询结果得到resultSet，封装到Map中
   * 方式2：SparkSQL方式   提供查询JDBC方式，Spark提供封装好的方法  √
   * @param sparkConf
   * @return
   */
  def queryProvinceInfo(sparkConf: SparkConf): mutable.Map[String, ProvinceInfo]={
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val properties: Properties = new Properties
    properties.setProperty("user", PropertiesUtil.getProperty("jdbc.user"))
    properties.setProperty("password", PropertiesUtil.getProperty("jdbc.password"))
    properties.setProperty("driver", PropertiesUtil.getProperty("jdbc.driver.name"))
    // type DataFrame = Dataset[Row]
    val dataFrame: DataFrame = sparkSession.read.jdbc(
      PropertiesUtil.getProperty("jdbc.url"),
      "base_province",
      properties
    )
    // 转为有类型的DataSet[ProvinceInfo]
    import sparkSession.implicits._
    val dataSet: Dataset[ProvinceInfo] = dataFrame.as[ProvinceInfo]   // 分布在Executor端
    val provinceInfos: mutable.HashMap[String, ProvinceInfo] = new mutable.HashMap[String, ProvinceInfo]()
    dataSet.collect().foreach(provinceInfo=>provinceInfos.put(provinceInfo.id, provinceInfo))
    provinceInfos
  }

  def main(args: Array[String]): Unit = {
    // 重写StreamingContext
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(appName).set("spark.testing.memory", "2147480000")
    // 添加ES相关参数（此处必须要添加，RDD.saveTOES才能生效
    sparkConf.set("es.nodes",PropertiesUtil.getProperty("es.nodes"))
    sparkConf.set("es.port",PropertiesUtil.getProperty("es.port"))
    sparkConf.set("es.index.auto.create", "true")  // 允许自动创建index
    sparkConf.set("es.nodes.wan.only", "true")  // 允许将主机名转换为ip
    val sparkContext = new SparkContext(sparkConf)
    // 重写
    context = new StreamingContext(sparkContext, Seconds(batchDuration))

    // 在Driver端查询省份维度信息
    val provinceMap: mutable.Map[String, ProvinceInfo] = queryProvinceInfo(sparkConf)
    // 广播
    // 广播后，不能使用provinceMap，而应该使用广播变量provinceMapBC
    // 广播和闭包的区别：广播是一个Executor复制一份，闭包是一个Task复制一份
    val provinceMapBC: Broadcast[mutable.Map[String, ProvinceInfo]] = context.sparkContext.broadcast(provinceMap)

    // 编写业务代码
    runApp{

      // 获取DStream
      val orderDetailDS: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(groupId, topic, context)
      val orderInfoDS: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(groupId, topic2, context)

      // 声明当前批次偏移量
      var ODRanges: Array[OffsetRange] = null
      var OIRanges: Array[OffsetRange] = null
      val ds1: DStream[(String, OrderDetail)] = orderDetailDS.transform(rdd => {
        // 获取当前消费到的这个批次偏移量
        ODRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 样例类转换
        rdd.map(record => {
          val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
          (orderDetail.order_id, orderDetail)
        })
      }
      )
      val ds2: DStream[(String, OrderInfo)] = orderInfoDS.transform(rdd => {
        // 获取当前消费到的这个批次偏移量
        OIRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 样例类转换
        rdd.map(record => {
          val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
          orderInfo.create_date = DataParseUtil.parseDateTimeStrToDate(orderInfo.create_time)
          orderInfo.create_hour = DataParseUtil.parseDateTimeStrToHour(orderInfo.create_time)
          (orderInfo.id, orderInfo)
        })
      }
      )

      // 实时的流中消费order_detail和order_info，进行join
      // 由于目前的网络延迟可能会造成od和oi两个流中的数据无法正确匹配，要保证join后依旧可以保留批次中的所有数据，再针对join的情况进行处理
      val ds3: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = ds2.fullOuterJoin(ds1)
      // ds3.print(1000)
      // ds4是处理网络延迟后封装的购物详情
      val ds4: DStream[SaleDetail] = ds3.mapPartitions(partitions => {
        // 获取Redis连接
        val jedis: Jedis = RedisUtil.getJedis
        // 构造一个集合，放封装好的SaleDetail
        val result: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
        // Gson
        val gson = new Gson()
        partitions.foreach {
          case (orderId, (orderInfoOption, orderDetailOption)) => {
            if (orderInfoOption != None) {
              val orderInfo: OrderInfo = orderInfoOption.get
              // 1. 和当前批次已消费到的order_detail关联
              if (orderDetailOption != None) {
                val orderDetail: OrderDetail = orderDetailOption.get
                result.append(new SaleDetail(orderInfo, orderDetail))
              }
              // 3. 将order_info写入到缓存，以防止后续有晚到的order_detail
              jedis.setex(PrefixConstant.order_info_redis_preffix + orderId, PrefixConstant.max_delay_time, gson.toJson(orderInfo))
              // 2. 到Redis缓存中去找之前已经早消费到order_detail，缓存里面有就关联
              // 如果Redis中set类型对应的key不存在，返回一个Set()，并不是Null
              val orderDetailSet: util.Set[String] = jedis.smembers(PrefixConstant.order_detail_redis_preffix + orderId)
              orderDetailSet.forEach(orderDetailStr => {
                val od: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
                result.append(new SaleDetail(orderInfo, od))
              })
            } else { // else 右边一定不为None，且左边一定为None
              val orderDetail: OrderDetail = orderDetailOption.get
              // 4. 和当前批次已消费到的order_info关联，如果order_info做过关联操作，此步骤可以省略。已经在1.中做过，此处跳过
              // 5. 无法关联的order_detail需要到Redis缓存中去找之前已经早消费到order_info，缓存里面有就关联
              // string类型，如果key不存在，返回的是null
              val orderInfoStr: String = jedis.get(PrefixConstant.order_info_redis_preffix + orderId)
              if (orderInfoStr != null) {
                val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
                result.append(new SaleDetail(orderInfo, orderDetail))
              } else {
                // 6. 如果5.中缓存无对应的order_info，则将order_detail写入到缓存，以防止后续有晚到的order_info
                jedis.sadd(PrefixConstant.order_info_redis_preffix + orderId, gson.toJson(orderDetail))
                // 设置Redis Key过期时间
                jedis.expire(PrefixConstant.order_info_redis_preffix + orderId, PrefixConstant.max_delay_time)
              }
            }
          }
        }
        jedis.close()
        result.toIterator
      })
      ds4.print(1000)

      // 关联维度数据，从Redis查询
      val ds5: DStream[SaleDetail] = ds4.mapPartitions(partition => {
        // 获取Redis连接
        val jedis: Jedis = RedisUtil.getJedis
        val result: Iterator[SaleDetail] = partition.map(saleDetail => {
          val userInfoStr: String = jedis.get(PrefixConstant.user_info_redis_preffix + saleDetail.user_id)
          if (userInfoStr != null) {
            // 合并用户信息
            saleDetail.mergeUserInfo(JSON.parseObject(userInfoStr, classOf[UserInfo]))
          }
          // 关联省份
          saleDetail.mergeProvinceInfo(provinceMapBC.value.get(saleDetail.province_id).get)
          // 返回关联维度后的购物明细
          saleDetail
        })
        jedis.close()
        result
      })

      // 写入ES数据库
      // 导入提供的静态方法
      import org.elasticsearch.spark._
      ds5.foreachRDD(rdd=>{
        rdd.cache()
        println("即将写入:" + rdd.count())
        // saveToEs(resource: String, cfg: scala.collection.Map[String, String])
        //    resource: 写入ES的哪个index
        //    cfg: scala.collection.Map[String, String]
        //      必须要配置：es.mapping.id->要写入ES的那个RDD中封装的类型的哪个属性作为_id
        rdd.saveToEs(DBNameConstant.SALEDETAILINDEX+LocalDate.now(), Map("es.mapping.id"->"order_detail_id"))
      })

      // 使用初始DS提交偏移量
//      if (null == ranges) {
//        println("偏移量:"+ ranges)
//        System.exit(1)
//      }
      //orderDetailDS.asInstanceOf[CanCommitOffsets].commitAsync(ODRanges)
      //orderInfoDS.asInstanceOf[CanCommitOffsets].commitAsync(OIRanges)
      if (ODRanges != null) {
        orderDetailDS.asInstanceOf[CanCommitOffsets].commitAsync(ODRanges)
      } else {
        println("ODRanges is null, skipping commit")
      }
      if (OIRanges != null) {
        orderInfoDS.asInstanceOf[CanCommitOffsets].commitAsync(OIRanges)
      } else {
        println("OIRanges is null, skipping commit")
      }

    }
  }

}
