package org.atguigu.sparkstreaming.demos

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* 消费多个主题，并将多个主题的数据进行关联。
* 每个主题一个流
*
* 只有DS[K,V]才能关联
* ----------------------------------
* select
* from t1 join t2 on t1.id=t2.id
* 在算子中，需要把关联的字段作为K才行
* DS[k,v1] join DS[k,v2] = DS[k,(v1,v2)]
* ---------------------
* 两个流，只有同一个批次的数据，才能关联
*
* Scala Option
*     Some 有
*     None 没有
*
* -----------------------------------------------
* 常见错误梳理：
*   1. 创建2个SparkStreamingContext：一个JVM只能有一个SparkContext对象
*   2. 只有同一个StreamingContext创建的DStream才能join
* */

object JoinDemo {
  val topic1 = "topicA"
  val topic2 = "topicB"
  def main(args: Array[String]): Unit = {
    // 创建 streamingContext 方式3
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transformDemo").set("spark.testing.memory", "2147480000")
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "20240506",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

//    val topics = Array("topicA")

    // 创建2个流
    val ds1: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array(topic1), kafkaParams)
    )

    val ds2: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array(topic2), kafkaParams)
    )

    val ds11: DStream[(String, Int)] = ds1.map(record => (record.value(), 1))
    val ds21: DStream[(String, String)] = ds2.map(record => (record.value(), "2"))

    // Join操作
    val ds3: DStream[(String, (Int, String))] = ds11.join(ds21)
    val ds4: DStream[(String, (Int, Option[String]))] = ds11.leftOuterJoin(ds21)
    val ds5: DStream[(String, (Option[Int], Option[String]))] = ds11.fullOuterJoin(ds21)

    // 输出：在屏幕打印
    //   print() 默认打印10行
//    ds3.print(1000)
    ds4.print(1000)

    // 启动APP
    streamingContext.start()

    // 阻塞进程，让进程一直运行
    streamingContext.awaitTermination()
  }
}
