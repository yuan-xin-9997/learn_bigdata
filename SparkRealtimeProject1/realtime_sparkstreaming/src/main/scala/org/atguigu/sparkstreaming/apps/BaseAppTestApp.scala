package org.atguigu.sparkstreaming.apps

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.atguigu.sparkstreaming.utils.DStreamUtil

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 15:56
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 */
object BaseAppTestApp extends BaseApp {

  // 重写消费者组、要消费的topic名
  override var groupId: String = "BaseAppTestApp"
  override var topic: String = "topicA"
  // 重写SparkStreaming App 名、采集时间周期
  override var appName: String = "BaseAppTestApp"
  override var batchDuration: Int = 10

  def main(args: Array[String]): Unit = {

    // 重写StreamingContext
    // 这里的Master写的是local[*]表示使用本地的集群，在线上应该改为yarn或null
    // val sparkConf = new SparkConf().setMaster("local[*]").setAppName(appName).set("spark.testing.memory", "2147480000")
    // bin/spark-submit --master yarn --class org.atguigu.sparkstreaming.apps.BaseAppTestApp 参数会默认放在SparkConf中
    val sparkConf = new SparkConf().setAppName(appName)
    val sparkContext = new SparkContext(sparkConf)
    context = new StreamingContext(sparkContext, Seconds(batchDuration))

    // 获取DStream
    val ds: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(groupId, topic, context)

    // 编写业务代码
    runApp{
      ds.map(record=>record.value()).print(1000)
    }
  }
}
