package org.atguigu.sparkstreaming.utils

import com.atguigu.realtime.utils.PropertiesUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 15:58
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 *
 *  获取Kafka Direct Streaming 工具类
 *
 * 如果要把offset提交到kafka：
 *  createDStream(groupId:String, topic:String, streamingContext:StreamingContext
 * 如果要把offset提交到mysql
 *               createDStream(groupId:String, topic:String, streamingContext:StreamingContext,isSaveOffsetToMysql:Boolean=true,offsetsMap: Map[TopicPartition, Long])
 */
object DStreamUtil {

  def createDStream(groupId:String, topic:String, streamingContext:StreamingContext,isSaveOffsetToMysql:Boolean=false,offsetsMap: Map[TopicPartition, Long]=null): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> PropertiesUtil.getProperty("kafka.broker.list") ,
      "key.deserializer" -> classOf[StringDeserializer],  // 此处写死
      "value.deserializer" -> classOf[StringDeserializer],  // 此处写死
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest", // 只影响第一次消费，第一次消费当前组没有指定offsets，才读取此参数。如果提交过了偏移量，下一次消费会
          // 读取从已经提交过的位置继续往后消费
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)

    var ds: InputDStream[ConsumerRecord[String, String]] = null

    if(isSaveOffsetToMysql){
      ds =  KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams, offsetsMap))
    }
    else{
      ds =  KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    }
    ds
  }

}
