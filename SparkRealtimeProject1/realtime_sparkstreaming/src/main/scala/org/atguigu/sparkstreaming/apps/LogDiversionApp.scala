package org.atguigu.sparkstreaming.apps

import com.atguigu.realtime.constants.TopicConstant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.atguigu.sparkstreaming.utils.DStreamUtil

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
 * at least once
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
      ds.foreachRDD(

      )
    }
  }
}
