package org.atguigu.sparkstreaming.output

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/*
* 演示把计算结果存入Redis
*   用string去存，一个单词是1个key
*   用hash去存
*     key: wordcount
*     value: hash
*       [filed,value]
*       word, count
* ---------------------------------------------
* def foreachRDD(foreachFunc: RDD[T] => Unit): Unit
*     foreachRDD和transform是类似的，都是将对DStream的运算转为对RDD的运算
*     foreachFunc 没有返回值的函数
*     foreachRDD也没有返回值
* ---------------------------------------------
* 读数据库：RDD.mapPartition 有返回值
* 写数据库：RDD.foreachPartition 没有返回值
* 两个算子都是以partition为单位操作（创建Connection）！
*
* */

object ForeachRDDDemo {
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

    res.cache()
    // 输出：在屏幕打印
    //   print() 默认打印10行
    res.print(1000)

    // 写入到Redis
    res.foreachRDD(rdd=>rdd.foreachPartition(partition=>{
      // 1个分区创建1个连接
      val jedis = new Jedis("hadoop102", 6379)
      jedis.auth("123456")
      // 使用连接
      partition.foreach{
        case (word, count)=>{
          val state = jedis.hget("wordcount", word)
          if (state == null){
            jedis.hset("wordcount", word, count.toString)
          }else{
            jedis.hset("wordcount", word, (count + state.toInt).toString)
          }
        }
      }
      // 关闭连接
      jedis.close()
    }))

    // 启动APP
    streamingContext.start()

    // 阻塞进程，让进程一直运行
    streamingContext.awaitTermination()
  }
}
