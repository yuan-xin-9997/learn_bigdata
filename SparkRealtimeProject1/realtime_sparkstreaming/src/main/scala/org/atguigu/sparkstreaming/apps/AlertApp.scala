package org.atguigu.sparkstreaming.apps

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.realtime.constants.{DBNameConstant, PrefixConstant, TopicConstant}
import com.atguigu.realtime.utils.{KafkaProducerUtil, PropertiesUtil, RedisUtil}
import com.google.gson.Gson
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.atguigu.sparkstreaming.beans.{ActionLog, CouponAlertInfo, OrderInfo, StartLog}
import org.atguigu.sparkstreaming.utils.{DStreamUtil, DataParseUtil, JDBCUtil}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
import org.atguigu.sparkstreaming.apps.DAUApp.{appName, batchDuration, context, groupId, parseBean, removeDuplicateLogInCommonBatch, removeDuplicateLogInHistoryBatch, runApp, topic}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.time.LocalDate
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 15:56
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 *
 *               Spark实时项目：实时预警需求
 *               非累加聚合类运算，一般都支持幂等性
 *               at least once + 幂等输出  保证精确计算一次
 *               采用ES存储运算结果，在SparkConf中需要添加相关参数
 *
 *               todo: 本scala代码，最终会报错，ranges为null，但是未排查到原因
 */
object AlertApp extends BaseApp {

  // 重写消费者组、要消费的topic名
  override var groupId: String = "AlertApp"
  override var topic: String = TopicConstant.ACTION_LOG
  // 重写SparkStreaming App 名、采集时间周期
  override var appName: String = "AlertApp"
  override var batchDuration: Int = 10

  def main(args: Array[String]): Unit = {
    // 重写StreamingContext
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(appName).set("spark.testing.memory", "2147480000")
    // 添加ES相关参数（此处必须要添加，RDD.saveTOES才能生效
    sparkConf.set("es.nodes",PropertiesUtil.getProperty("es.nodes"))
    sparkConf.set("es.port",PropertiesUtil.getProperty("es.port"))
    sparkConf.set("es.index.auto.create", "true")  // 允许自动创建index
    sparkConf.set("es.nodes.wan.only", "true")  // 允许将主机名转换为ip
    val sparkContext = new SparkContext(sparkConf)
    context = new StreamingContext(sparkContext, Seconds(batchDuration))

    // 编写业务代码
    runApp{

      // 获取DStream
      val ds: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(groupId, topic, context)

      // 声明当前批次偏移量
      var ranges: Array[OffsetRange] = null
      val ds1: DStream[ActionLog] = ds.transform(rdd => {
        // 获取当前消费到的这个批次偏移量
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 样例类转换
        rdd.map(record => {
          val actionLog: ActionLog = JSON.parseObject(record.value(), classOf[ActionLog])
          actionLog
        })
      }
      )

      // 对流进行开窗
      // 一般情况下提交的间隔和窗口是一样的。为了实现预警效果，设置提交时间为30s，计算窗口期为5分钟，会造成重复计算
      val ds2: DStream[ActionLog] = ds1.window(Minutes(5), Seconds(30))

      // 步骤2:按照设备和用户分组
      val ds3: DStream[((String, String), Iterable[ActionLog])] = ds2.map(actionLog => ((actionLog.mid, actionLog.uid), actionLog)).groupByKey()

      // 步骤3:根据需求，过滤得到修改收货地址的actionsLog
      val ds4: DStream[((String, String), Iterable[ActionLog])] = ds3.filter {
        case ((mid, uid), logs) => {
          var flag = false // 当前用户在当前设备的活动是否有增加收货地址
          Breaks.breakable {
            logs.foreach(log => {
              if ("trade_add_address".equals(log.action_id)) {
                flag = true
                // 没有必要继续判断本批次的其他actionsLog
                Breaks.break()
              }
            })
          }
          flag
        }
      }

      // 步骤4:根据设备分组，统计用户数量，
      //超过2个用户，则生成预警日志
      // ds5的数据结构
      //        (String, Iterable[Iterable[ActionLog]])
      //        (mid1,  [
      //            [
      //               user1log1,user1log2,user1log3
      //            ],
      //            [
      //                user2log1,user2log2,user2log3
      //            ],
      //            [
      //                ...
      //            ],
      //        ]
      //        )
      val ds5: DStream[(String, Iterable[Iterable[ActionLog]])] = ds4.map {
        case ((mid, uid), logs) => (mid, logs)
      }.groupByKey()
      // 过去5分钟，设备上登录的增加收货地址的用户个数超过2的设备
      val ds6: DStream[(String, Iterable[Iterable[ActionLog]])] = ds5.filter(_._2.size >= 2)
      val ds7: DStream[(String, Iterable[ActionLog])] = ds6.mapValues(_.flatten)

      // 生成预警日志
      val ds8: DStream[Unit] = ds7.map {
        case (mid, logs) => {
          val uids: mutable.Set[String] = new mutable.HashSet[String]()
          val itemIds: mutable.Set[String] = new mutable.HashSet[String]()
          val events: ListBuffer[String] = new ListBuffer[String]()
          logs.foreach(log => {
            uids.add(log.uid)
            events.append(log.action_id)
            if ("favor_add".equals(log.action_id)) {
              itemIds.add(log.item)
            }
            // ts
            val ts: Long = System.currentTimeMillis()
            // id: 需要体现mid，mid_YYYY-MM-DD_HH_mm
            // 且，同一设备，如果一分钟产生多条预警，只保留最后一天预警日志
            val id: String = mid + "_" + DataParseUtil.parseMillTsToDateTimeWithoutSeconds(ts)
            CouponAlertInfo(id, uids, itemIds, events, ts)
          })
        }
      }

      // 写入ES数据库
      // 导入提供的静态方法
      //    数据漂移问题：不属于这一天的数据写入了这一天的集合中
      //    例如 mid1_2022-07-20 23:59写入到了DBNameConstant.ALERTINDEX2022-07-21
      //    ES不用解决，如果是Hive则需要解决数据飘逸问题。因为ES在做运算的时候是以数据的ts为标准，不是以INDEX的name当中的日期时间为标准
      //    --------------
      //    从根本上解决漂移问题
      //    不用 saveToEs，而是自己写代码写入ES。需要额外学习ES提供的JavaAPI
      //rdd.foreachPartition(partition => {
      ////创建到ES的连接
      //3
      //）
      //partition.foreach( data => JestClient.execute(Action (data)))
      //关闭连接
      //I
      import org.elasticsearch.spark._
      ds8.foreachRDD(rdd=>{
        println("即将写入:" + rdd.count())
        // saveToEs(resource: String, cfg: scala.collection.Map[String, String])
        //    resource: 写入ES的哪个index
        //    cfg: scala.collection.Map[String, String]
        //      必须要配置：es.mapping.id->要写入ES的那个RDD中封装的类型的哪个属性作为_id
        rdd.saveToEs(DBNameConstant.ALERTINDEX+LocalDate.now(), Map("es.mapping.id"->"id"))
      })

      // 提交偏移量
//      if (null == ranges) {
//        println("偏移量:"+ ranges)
//        System.exit(1)
//      }
//      ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
      if (ranges != null) {
        ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
      } else {
        println("ranges is null, skipping commit")
      }

    }
  }

}
