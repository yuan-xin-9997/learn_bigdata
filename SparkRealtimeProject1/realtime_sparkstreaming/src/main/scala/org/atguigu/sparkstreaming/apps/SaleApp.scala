package org.atguigu.sparkstreaming.apps

import java.time.LocalDate
import java.util
import java.util.Properties
import com.alibaba.fastjson.JSON
import com.atguigu.realtime.constants.{PrefixConstant, TopicConstant}
import com.atguigu.realtime.constants.{DBNameConstant, PrefixConstant, TopicConstant}
import com.atguigu.realtime.utils.{PropertiesUtil, RedisUtil}
import com.atguigu.realtime.utils.{PropertiesUtil, RedisUtil}
import com.google.gson.Gson
import javafx.scene.chart.PieChart.Data
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.atguigu.sparkstreaming.apps.SaleDetailApp.{context, groupId, topic, topic2}
import org.atguigu.sparkstreaming.beans.{OrderDetail, OrderInfo, ProvinceInfo, SaleDetail, UserInfo}
import org.atguigu.sparkstreaming.utils.{DStreamUtil, DataParseUtil}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case object SaleApp extends BaseApp {
  override var appName: String = "SaleDetailApp"
  override var batchDuration: Int = 30
  override var groupId: String = "hahaha"
  override var topic: String = TopicConstant.ORDER_INFO
  val orderDetailTopic: String = TopicConstant.ORDER_DETAIL

  def main(args: Array[String]): Unit = {

    //设置ES的相关参数
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(appName).set("spark.testing.memory", "2147480000")


    sparkConf.set("es.nodes",PropertiesUtil.getProperty("es.nodes"))
    sparkConf.set("es.port",PropertiesUtil.getProperty("es.port"))
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes.wan.only", "true")

    context = new StreamingContext(sparkConf,Seconds(batchDuration))

    val provinceMap: Map[String, ProvinceInfo] = queryProvinceInfo(sparkConf)
    //广播省份信息
    val provinceBc: Broadcast[Map[String, ProvinceInfo]] = context.sparkContext.broadcast(provinceMap)

    runApp {

      //获取两个流
      val orderDetailDs: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(groupId, topic, context)
      val orderInfoDs: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(groupId, topic2, context)

      //两个偏移量
      var orderInfoRanges: Array[OffsetRange] = null
      var orderDeetailRanges: Array[OffsetRange] = null

      //获取偏移量，封装Bean
      val ds1: DStream[(String, OrderInfo)] = orderInfoDs.transform(rdd => {

        orderInfoRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        parseOrderInfo(rdd)

      })

      val ds2: DStream[(String, OrderDetail)] = orderDetailDs.transform(rdd => {

        orderDeetailRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd.map(record => {
          val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
          (orderDetail.order_id, orderDetail)
        })

      })

      // 将流关联后，根据关联后的数据关联情况判断是否Join上了，再进行处理。保证关联后双方的数据都要保留
      val ds3: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = ds1.fullOuterJoin(ds2)

      // 将 Order_info和order_detail关联后的数据
      val ds4: DStream[SaleDetail] = ds3.mapPartitions(partition => {

        val jedis: Jedis = RedisUtil.getJedis

        //准备存放关联成功的SaleDetail的集合
        val result: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

        val gson = new Gson()

        partition.foreach {
          case (orderId, (orderInfoOption, orderDetailOption)) => {

            if (orderInfoOption.isDefined) {

              val orderInfo: OrderInfo = orderInfoOption.get

              if (orderDetailOption.isDefined) {

                val orderDetail: OrderDetail = orderDetailOption.get

                // ①和当前批次已经到达的Order_detail关联
                result.append(new SaleDetail(orderInfo, orderDetail))

              }

              // ②可能有早到的order_detail,去缓存中查询，查到就关联
              //  如果是set集合，key不存在，返回Set()
              val orderDetailStrs: util.Set[String] = jedis.smembers(PrefixConstant.order_detail_redis_preffix + orderId)

              orderDetailStrs.forEach(str => {

                val detail: OrderDetail = JSON.parseObject(str, classOf[OrderDetail])

                result.append(new SaleDetail(orderInfo, detail))

              })

              // ③无条件写入缓存，以防后续后晚到的order_detail
              jedis.setex(PrefixConstant.order_info_redis_preffix + orderId, PrefixConstant.max_delay_time,gson.toJson(orderInfo))

            } else {

              // 在else中order_info为none，那么orderDetail一定不为none
              val orderDetail: OrderDetail = orderDetailOption.get

              //进一步确认，取缓存找对应的Order_info，找到，就关联。
              // 如果是string类型，key不存在，value是null
              val orderInfoStr: String = jedis.get(PrefixConstant.order_info_redis_preffix + orderId)

              if (orderInfoStr != null) {

                result.append(new SaleDetail(JSON.parseObject(orderInfoStr, classOf[OrderInfo]), orderDetail))
              } else {

                //找不到，说明order_detail来早了，自己写入缓存，等待后续到达的order_info关联
                jedis.sadd(PrefixConstant.order_detail_redis_preffix + orderId, gson.toJson(orderDetail))

                jedis.expire(PrefixConstant.order_detail_redis_preffix + orderId,PrefixConstant.max_delay_time)

              }
            }
          }
        }

        jedis.close()

        result.iterator


      })

      // 关联用户和省份
      val result: DStream[SaleDetail] = ds4.mapPartitions(partition => {

        val jedis: Jedis = RedisUtil.getJedis()

        val saleDetails: Iterator[SaleDetail] = partition.map(saleDetail => {

          val userStr: String = jedis.get(PrefixConstant.user_info_redis_preffix + saleDetail.user_id)

          val userInfo: UserInfo = JSON.parseObject(userStr, classOf[UserInfo])

          //关联用户信息
          saleDetail.mergeUserInfo(userInfo)

          //关联省份信息
          saleDetail.mergeProvinceInfo(provinceBc.value.get(saleDetail.province_id).get)

          saleDetail

        })

        jedis.close()

        saleDetails

      })

      import org.elasticsearch.spark._

      result.foreachRDD(rdd => {

        println("即将写入:"+rdd.count())

        rdd.saveToEs("realtime2022_sale_detail_"+LocalDate.now,Map("es.mapping.id" -> "order_detail_id"))

        //提交偏移量
        orderInfoDs.asInstanceOf[CanCommitOffsets].commitAsync(orderInfoRanges)
        orderDetailDs.asInstanceOf[CanCommitOffsets].commitAsync(orderDeetailRanges)

      })

    }


  }

  //查询省份信息
  def queryProvinceInfo(sparkConf: SparkConf):Map[String,ProvinceInfo]={

    val result = new mutable.HashMap[String, ProvinceInfo]()

    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val properties = new Properties()

    properties.setProperty("driver",PropertiesUtil.getProperty("jdbc.driver.name"))
    properties.setProperty("user",PropertiesUtil.getProperty("jdbc.user"))
    properties.setProperty("password",PropertiesUtil.getProperty("jdbc.password"))

    val df: DataFrame = session.read.format("jdbc").jdbc(PropertiesUtil.getProperty("jdbc.url"), "base_province", properties)

    import session.implicits._

    df.as[ProvinceInfo].collect().foreach(province => {
      result.put(province.id,province)
    })

    result.toMap

  }

  def parseOrderInfo(rdd: RDD[ConsumerRecord[String, String]]):RDD[(String,OrderInfo)] = {

    rdd.mapPartitions(partition => {

      partition.map(record => {

        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        // create_time": "2022-05-25 02:23:00
        //为create_date和create_hour赋值
        orderInfo.create_date = DataParseUtil.parseDateTimeStrToDate(orderInfo.create_time)
        orderInfo.create_hour = DataParseUtil.parseDateTimeStrToHour(orderInfo.create_time)

        (orderInfo.id,orderInfo)

      })

    })

  }
}