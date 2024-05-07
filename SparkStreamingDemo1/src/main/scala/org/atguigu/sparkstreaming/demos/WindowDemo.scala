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
* 在SparkStreaming中有3个和时间相关的参数
*   batchDuration Duration：将多长时间消费到的数据封装为一个RDD。一个批次的采集时间
*   window Duration：要计算的数据的时间范围
*   slide Duration：多久提交一次job运算
*
*   默认情况，当没指定window和slide，他们都等同于batchDuration。batchDuration在构造StreamingContext时必须指定
*       即，消费10s的数据作为1个批次，每间隔10s，提交1个job，这个job算过去10s的数据
* ---------------------------------------------
* 在获取流的任意位置指调用window()
*   Params:
      windowDuration – width of the window; must be a multiple of this DStream's batching interval
      slideDuration – sliding interval of the window (i.e., the interval after which
      * the new DStream will generate RDDs); must be a multiple of this DStream's batching interval
* windowDuration和slideDuration必须是batchDuration的整数倍
* ----------------------------------------------
* Kafka ConsumerRecord is not serializable. Use .map to extract fields before calling .persist or .window
* 解决方法：
*   方法1：不要在获取初始了初始的ds: InputDStream[ConsumerRecord[String, String]] 就立刻window，而应该先取出ConsumerRecord的
*     value，之后再window
*   方法2：将ConsumerRecord使用kryo进行序列化。参考SparkCore课件
* ----------------------------------------------
* 每间隔1分钟，计算过去2分钟产生的数据！
*   window=2
*   slide=1
*   batchDuration只要是1分钟的因子
* */

object WindowDemo {
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

    // 错误示范
    //val ds1: DStream[ConsumerRecord[String, String]] = ds.window(Seconds(10), Seconds(10))

    // (String, Int):(word, n)
    val ds1: DStream[(String, Int)] = ds.flatMap(record => record.value().split(" "))
      .map((_, 1)).reduceByKey(_+_)

    val ds2: DStream[(String, Int)] = ds1.window(Seconds(10), Seconds(10))

    val res: DStream[(String, Int)] = ds2.transform(rdd => rdd.sortByKey())

    // 输出：在屏幕打印
    //   print() 默认打印10行
    res.print(1000)

    // 启动APP
    streamingContext.start()

    // 阻塞进程，让进程一直运行
    streamingContext.awaitTermination()
  }
}
