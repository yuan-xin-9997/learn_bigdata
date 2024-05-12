package org.atguigu.sparkstreaming.apps

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.realtime.constants.TopicConstant
import com.atguigu.realtime.utils.KafkaProducerUtil
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.atguigu.sparkstreaming.utils.DStreamUtil

import java.util

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 15:56
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 *
 * Spark实时项目：日志分流程序
 *    从Kafka的base_log中读取日志，调出其中start_log,action_log(炸裂，拆分)，将数据写回kafka
 *    类似离线数仓：ods_log_inc输入->hql->挑选page_log->写入dwd_traffic_page_view_inc
 *    离线数仓的HDFS 对应 实时项目的Kafka
 *
 * ------------------------------------
 * at least once + 幂等输出（kafka producer 开启输出幂等配置）
 */
object LogDiversionApp extends BaseApp {

  // 重写消费者组、要消费的topic名
  override var groupId: String = "LogDiversionApp"
  override var topic: String = TopicConstant.ORIGINAL_LOG
  // 重写SparkStreaming App 名、采集时间周期
  override var appName: String = "LogDiversionApp"
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
          // 处理
          handleLog(rdd)
          // 提交offsets
          ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
        }
      }
      )
    }
  }

  /**
   * 挑选start_log,action_log(拆分），并将json字符串写到Kafka
   * 只要读写数据库/中间件，以分区操作为单位
   * @param rdd
   */
  private def handleLog(rdd: RDD[ConsumerRecord[String, String]]) = {
    rdd.foreachPartition(partition=>{
      // 以分区为单位创建gson
      val gson = new Gson()

      partition.foreach(record=>{
        // 取出Kafka中的value
        val jSONObject: JSONObject = JSON.parseObject(record.value())
        if(jSONObject.containsKey("start") && !jSONObject.containsValue("err")){
          // 这是一条启动过日志
          KafkaProducerUtil.sendData(gson.toJson(jSONObject), TopicConstant.STARTUP_LOG)
        }else if (jSONObject.containsKey("actions") && !jSONObject.containsValue("err")){
          // 折是一条含有actions的日志，需要把actions炸裂，取出每一条action，再和当前这条日志的common、page部分拼接成一条新的jsonstr，
          // 写到kafka
          // 获取common部分
          val commonMap: util.Map[String, AnyRef] = JSON.parseObject(jSONObject.getString("common")).getInnerMap
          val pageMap: util.Map[String, AnyRef] = JSON.parseObject(jSONObject.getString("page")).getInnerMap
          val actionsStr: String = jSONObject.getString("actions")
          parseActions(commonMap,pageMap,actionsStr,gson)
        }
      })

      // 着急，可以立刻flush缓冲区
      KafkaProducerUtil.flush()
    })
  }

  private def parseActions(commonMap: util.Map[String, AnyRef], pageMap: util.Map[String, AnyRef], actionsStr: String,gson:Gson) = {
    val jSONArray: JSONArray = JSON.parseArray(actionsStr)
    for(i<-0 until jSONArray.size){
      val actionStr: String = jSONArray.getString(i)
      val actionMap: util.Map[String, AnyRef] = JSON.parseObject(actionStr).getInnerMap
      // 3个Map合并，3个Map中有重名的key（后合并会覆盖前面的），要注意顺序，否则无所谓
      actionMap.putAll(commonMap)
      actionMap.putAll(pageMap)
      KafkaProducerUtil.sendData(gson.toJson(actionMap), TopicConstant.ACTION_LOG)
    }
  }
}
