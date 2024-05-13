package org.atguigu.sparkstreaming.apps

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.realtime.constants.{PrefixConstant, TopicConstant}
import com.atguigu.realtime.utils.{KafkaProducerUtil, RedisUtil}
import com.google.gson.Gson
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.atguigu.sparkstreaming.beans.StartLog
import org.atguigu.sparkstreaming.utils.{DStreamUtil, DataParseUtil}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

import java.util

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 15:56
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 *
 * Spark实时项目：每日设备首次启动行为分析Application
 *
 * ------------------------------------
 * at least once + 幂等输出（HBase）
 */
object DAUApp extends BaseApp {

  // 重写消费者组、要消费的topic名
  override var groupId: String = "DAUApp"
  override var topic: String = TopicConstant.STARTUP_LOG
  // 重写SparkStreaming App 名、采集时间周期
  override var appName: String = "DAUApp"
  override var batchDuration: Int = 10

  def main(args: Array[String]): Unit = {

    // 重写StreamingContext
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(appName).set("spark.testing.memory", "2147480000")
    val sparkContext = new SparkContext(sparkConf)
    context = new StreamingContext(sparkContext, Seconds(batchDuration))

    // 获取DStream
    val ds: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(groupId, topic, context)

    // 编写业务代码
    runApp{
      ds.foreachRDD(rdd => {
        if(!rdd.isEmpty()){
          // 获取当前消费到的偏移量
          val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          // 封装样例类
          val rdd1: RDD[StartLog] = parseBean(rdd)
          // 同批次去重
          val rdd2: RDD[StartLog] = removeDuplicateLogInCommonBatch(rdd1)
          // 使用Redis过滤已经记录过的设备（历史批次去重）
          val rdd3: RDD[StartLog] = removeDuplicateLogInHistoryBatch(rdd2)
          //  启动日志写入HBase（具有幂等性，第二次写会覆盖第一次）
          //        saveToPhoenix(tableName: String, cols: Seq[String],
          //                           conf: Configuration = new Configuration, zkUrl: Option[String] = None, tenantId: Option[String] = None)
          //             tableName: HBase表名
          //             cols: Seq[String] Seq(序列，有顺序的集合)。
          //             conf: Configuration = new Configuration
          //                 HBaseConfiguration.create()： 先new Configuration，再读取hbase-site.xml和hbase-default.xml
          //             zkUrl: Option[String] = None     base用的zk的地址
          rdd3.saveToPhoenix(
            "REALTIME2022_STARTLOG",
            Seq("ID","OPEN_AD_MS","OS","CH","IS_NEW","MID","OPEN_AD_ID","VC","AR",
              "UID","ENTRY","OPEN_AD_SKIP_MS","MD","LOADING_TIME","BA","TS","START_DATE","START_TIME"),
            HBaseConfiguration.create(),
            Some("hadoop103:2181")
          )
          rdd3.cache()
          println("即将写入:"+rdd3.count())
          //  将写入Hbase的设备ID记录在Redis（具有幂等性，sadd会去重）
          rdd3.foreachPartition(
            partition=>{
              val jedis: Jedis = RedisUtil.getJedis
              partition.foreach(log=>jedis.sadd(PrefixConstant.dau_redis_Preffix + log.start_date, log.mid))
              jedis.close()
            }
          )
          // 提交offsets
          ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
        }
      }
      )
    }
  }

  /**
   * 将从Kafka读取的start log主题数据封装为StartLog
   * ---------------------------------------------
   * fastjson封装规则：
   *    要求封装的jsonstr 和 java bean要一一对应
   *    jsonstr有几个一级属性，javabean有和它同样的一级属性，属性名必须一致（大小写一致）
   *
   * @param rdd
   * @return
   */
  private def parseBean(rdd: RDD[ConsumerRecord[String, String]]):RDD[StartLog] = {
    rdd.map(record=>{
      val startLog: StartLog = JSON.parseObject(record.value(), classOf[StartLog])
      // 继续封装id,starttime,startdate
      startLog.start_date = DataParseUtil.parseMillTsToDate(startLog.ts.toLong)
      startLog.start_time = DataParseUtil.parseMillTsToDateTime(startLog.ts.toLong)
      startLog.id = startLog.start_time + "_" + startLog.mid
      startLog
    })
  }

  /**
   * 同批次去重：
   * 2.对当前批次的启动日志按照mid_id和date进行同批次去重，每个设备只取当前批次当前date启动时间最早的那条
   *
   * 数据演示：
   *
   * 当前这批：
   *
   * {(mid1,2022-07-19), log1} {(mid1, 2022-07-19), log2} {(mid1,2022-07-19), log3} {(mid2, 2022-07-19), log4}
   *
   * ]
   *
   * 分组后：
   *
   * ((mid1, 2022-07-19), Iterable[log1,log2,log3])
   *
   * ((mid2, 2022-07-19), Iterable[log4)
   *
   * flatMap:
   *
   * Iterable[log1,log2,log3] ---> List[log1,log2,log3] ---> List[log2,log1,log3] --->List[log2]
   *
   * @param rdd
   * @return
   */
  private def removeDuplicateLogInCommonBatch(rdd: RDD[StartLog]):RDD[StartLog] = {
    val rdd1: RDD[((String, String), StartLog)] = rdd.map(log => ((log.mid, log.start_date), log))

    // 方式一：效率低下
    // groupByKey:将map阶段的所有数据都shuffle，分组！！！
    // 当前的需求：分组聚合(reduceByKey),即1个组-->1条数据
    // reduceByKey: 可以map端聚合
//    val rdd2: RDD[((String, String), Iterable[StartLog])] = rdd1.groupByKey()
//    val rdd3: RDD[StartLog] = rdd2.flatMap{
//      case ((mid, date), logs:Iterable[StartLog])=>logs.toList.sortBy(_.ts).take(1)
//    }

    // 方式2：使用reduceByKey
    val rdd2: RDD[((String, String), StartLog)] = rdd1.reduceByKey((log1: StartLog, log2: StartLog) => {
      if (log1.ts < log2.ts) {
        log1
      } else {
        log2
      }
    })
    val rdd3: RDD[StartLog] = rdd2.values
    rdd3
  }

  /**
   * 3.对当前批次的启动日志按照mid_id进行历史批次去重，将每天已经记录过的历史设备在本批次的启动日志过滤掉
   *
   * 设计数据在Redis中的K-V结构
   *     数据： 日期(哪一天) 设备id（哪些设备已经记录过最早启动日志了）
   *     K: 字符串
   *     V:
   *        单值: 1:1 string hash
   *        多值: 1:N list set zset
   *     参考粒度:
   *        1天的1个设备是1个K-V
   *            K:日期_设备ID
   *            V:随便
   *        1天的N个设备室1个K-V
   *            K:日期
   *            V:Set(mid)
   *     使用场景：从一批数据中判断当前这批数据，哪些mid已经在redis中存在
   *     决策：
   *        是否方便管理的角度：采取 1天的N个设备室1个K-V 方式
   * @param rdd
   * @return
   */
  private def removeDuplicateLogInHistoryBatch(rdd: RDD[StartLog]):RDD[StartLog] = {
    rdd.mapPartitions(partitions=>{
      val jedis: Jedis = RedisUtil.getJedis
      //val filterLogs=partitions.filter{log:StartLog=>!jedis.sismember(PrefixConstant.dau_redis_Preffix+log.start_date,log.mid)}
      val filterLogs: Iterator[StartLog] = partitions.filter(
        log => !jedis.sismember(PrefixConstant.dau_redis_Preffix + log.start_date, log.mid)
      )
      jedis.close()
      filterLogs
    })
  }

}
