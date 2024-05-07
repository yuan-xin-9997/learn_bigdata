package org.atguigu.sparkstreaming.output

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* 保存到文件
*
* Windows不能运行shell命令
* */

object SaveAsTextFileDemo {
  def main(args: Array[String]): Unit = {
    // 创建 streamingContext 方式3
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transformDemo").set("spark.testing.memory", "2147480000")
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "20240506",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("topicA")

    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // (String, Int):(word, n)
    val ds1: DStream[(String, Int)] = ds.flatMap(record => record.value().split(" "))
      .map((_, 1)).reduceByKey(_+_)

    val ds2: DStream[(String, Int)] = ds1.window(Seconds(10), Seconds(10))

    val res: DStream[(String, Int)] = ds2.transform(rdd => rdd.sortByKey())

    // 输出：在屏幕打印
    //   print() 默认打印10行
//    res.print(1000)

    // 以文件形式输出
    res.saveAsTextFiles("window", "log")

    // 启动APP
    streamingContext.start()

    // 阻塞进程，让进程一直运行
    streamingContext.awaitTermination()
  }
}
