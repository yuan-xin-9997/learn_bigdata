package org.atguigu.sparkstreaming.apps

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.realtime.constants.{PrefixConstant, TopicConstant}
import com.atguigu.realtime.utils.{KafkaProducerUtil, RedisUtil}
import com.google.gson.Gson
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.atguigu.sparkstreaming.beans.{OrderInfo, StartLog}
import org.atguigu.sparkstreaming.utils.{DStreamUtil, DataParseUtil, JDBCUtil}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util
import scala.collection.mutable

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 15:56
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 *
 * Spark实时项目：每日分时GMV分析
 *      聚合类运算，属于累加，没有幂等性
 *      at least once + 事务  保证精确计算一次
 *      
 *
 * ------------------------------------
 * 设计表
 *               CREATE TABLE `gmvstats` (
 *               `stat_date` DATE NOT NULL,
 *               `stat_hour` VARCHAR(10) NOT NULL,
 *               `gmv` DECIMAL(20,2) DEFAULT NULL,
 *               PRIMARY KEY (`stat_date`,`stat_hour`)
 *               ) ENGINE=INNODB DEFAULT CHARSET=utf8
 * ------------------------------------
 * 如果当前希望从Kafka topic REALTIME_DB_ORDER_INFO的最新位置开始消费，需要把"auto.offset.reset" -> "latest"
 * 如果当前希望从最早位置开始消费，需要到Mysql偏移量表中初始化数据：
 *               GMVApp,REALTIME_DB_ORDER_INFO,0,57
 *               GMVApp,REALTIME_DB_ORDER_INFO,1,47
 *               GMVApp,REALTIME_DB_ORDER_INFO,2,56
 */
object GMVApp extends BaseApp {

  // 重写消费者组、要消费的topic名
  override var groupId: String = "GMVApp"
  override var topic: String = TopicConstant.ORDER_INFO
  // 重写SparkStreaming App 名、采集时间周期
  override var appName: String = "GMVApp"
  override var batchDuration: Int = 10

  /*
  *
  * 查询MySQL中已经提交的偏移量，根据当前组所消费的topic去查询
  * */
  def selectOffsetsFromMysql(groupId:String, topic:String):Map[TopicPartition, Long]={
    var connection: Connection = null
    var ps: PreparedStatement = null
    val sql =
      """
        |select
        | partitionId,
        | offset
        |from offsets
        |where groupId=? and topic=?
        |""".stripMargin
    var offsets = new mutable.HashMap[TopicPartition, Long]()
    try {
      connection = JDBCUtil.getConnection()
      ps = connection.prepareStatement(sql)
      ps.setString(1,groupId)
      ps.setString(2,topic)
      val resultSet: ResultSet = ps.executeQuery()
      while (resultSet.next()){
        offsets.put(new TopicPartition(topic, resultSet.getInt("partitionId")), resultSet.getLong("offset"))
      }
    } catch {
      case e:Exception =>{
        e.printStackTrace()
        throw new RuntimeException("查询偏移量失败")
      }
    } finally {
      if (ps != null){
        ps.close()
      }
      if (connection != null){
        connection.close()
      }
    }
    // 可变转不可变
    offsets.toMap
  }

  private def writeResultAndOffsetsInCommonTransaction(result: Array[((String, String), Double)], ranges: Array[OffsetRange]): Unit = {
    // 累加GMV
    val sql1 =
      """
        |insert into spark_realtime.gmvstats values (?, ?, ?)
        |on duplicate key update gmv=gmv+values(gmv)
        |""".stripMargin
    // 写偏移量
    val sql2 =
      """
        |insert into offsets values (?, ?, ?, ?)
        |on duplicate key update offset=values(offset)
        |""".stripMargin
//    val sql3 =
//      """
//        |replace into offsets values (?, ?, ?, ?)
//        |""".stripMargin

    var connection: Connection = null
    var ps1: PreparedStatement = null
    var ps2: PreparedStatement = null
    try {
      connection = JDBCUtil.getConnection()
      // 开启事务（即取消事务自动提交，改为手动提交
      connection.setAutoCommit(false)
      ps1 = connection.prepareStatement(sql1)
      ps2 = connection.prepareStatement(sql2)
      for (((date, hour), gmv) <- result) {
        ps1.setString(1,date)
        ps1.setString(2,hour)
        ps1.setDouble(3,gmv)
        // 将sql攒起来
        ps1.addBatch()
      }
      for (offSetRange <- ranges) {
        ps2.setString(1,groupId)
        ps2.setString(2,offSetRange.topic)
        ps2.setInt(3,offSetRange.partition)
        ps2.setLong(4,offSetRange.untilOffset)
        ps2.addBatch()
      }
      val res1: Array[Int] = ps1.executeBatch()
      val res2: Array[Int] = ps2.executeBatch()
      // 手动提交事务
      connection.commit()
      println("数据写入："+res1.size)
      println("偏移量写入："+res2.size)
    } catch {
      case e:Exception =>{
        // 回滚事务
        connection.rollback()
        e.printStackTrace()
        throw new RuntimeException("查询偏移量失败")
      }
    } finally {
      if (ps1 != null){
        ps1.close()
      }
      if (ps2 != null){
        ps1.close()
      }
      if (connection != null){
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {

    // 重写StreamingContext
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(appName).set("spark.testing.memory", "2147480000")
    val sparkContext = new SparkContext(sparkConf)
    context = new StreamingContext(sparkContext, Seconds(batchDuration))

    // 从MySQL查询上次提交的偏移量
    val offsetMap: Map[TopicPartition, Long] = selectOffsetsFromMysql(groupId, topic)

    // 获取DStream
    //    并从刚刚查询的位置向后消费的流
    val ds: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(groupId, topic, context, true, offsetMap)

    // 编写业务代码
    runApp{
      ds.foreachRDD(rdd => {
        if(!rdd.isEmpty()){
          // 获取当前消费到的偏移量
          val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          // 封装样例类
          val rdd1: RDD[OrderInfo] = parseBean(rdd)

          // GMV计算
          // 当前批次在每一天每个小时成交的总额
          val result: Array[((String, String), Double)] = rdd1.map(orderInfo => ((orderInfo.create_date, orderInfo.create_hour), orderInfo.total_amount))
            .reduceByKey(_ + _)
            .collect()

          // 将计算结果和偏移量写入数据库
          writeResultAndOffsetsInCommonTransaction(result, ranges)
        }else{
          println("未获取到数据")
        }
      }
      )
    }
  }

  /**
   * 解析样例类
   * @param rdd
   * @return
   */
  private def parseBean(rdd: RDD[ConsumerRecord[String, String]]):RDD[OrderInfo] = {

    rdd.map(record=> {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo.create_date = DataParseUtil.parseDateTimeStrToDate(orderInfo.create_time)
      orderInfo.create_hour = DataParseUtil.parseDateTimeStrToHour(orderInfo.create_time)
      orderInfo
      }
    )
  }

}
