import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import com.alibaba.fastjson.JSON
import com.atguigu.realtime.constants.TopicConstant
import com.atguigu.realtime.utils.PropertiesUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.atguigu.sparkstreaming.apps.AlertApp.{context, groupId, topic}
import org.atguigu.sparkstreaming.apps.BaseApp
import org.atguigu.sparkstreaming.beans.{ActionLog, CouponAlertInfo}
import org.atguigu.sparkstreaming.utils.DStreamUtil

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

object AlertApp1 extends BaseApp {
  override var appName: String = "AlertApp"
  override var batchDuration: Int = 30
  override var groupId: String = "realtime1227"
  override var topic: String = TopicConstant.ACTION_LOG

  def main(args: Array[String]): Unit = {


    //设置ES的相关参数
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(appName).set("spark.testing.memory", "2147480000")

    sparkConf.set("es.nodes",PropertiesUtil.getProperty("es.nodes"))
    sparkConf.set("es.port",PropertiesUtil.getProperty("es.port"))
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes.wan.only", "true")

    context = new StreamingContext(sparkConf,Seconds(batchDuration))

    runApp{

      //获取流
      val ds: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(groupId, topic, context)
      var ranges: Array[OffsetRange] = null

      //使用transform
      val ds1: DStream[ActionLog] = ds.transform(rdd => {

        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //将rdd解析为样例类
        rdd.map(record => JSON.parseObject(record.value(), classOf[ActionLog]))


      })

      //采集过去5min的数据
      val ds2: DStream[ActionLog] = ds1.window(Minutes(5))

      //按照用户和设备进行分组，把每个用户在过去5min在每个设备上的所有动作汇总
      val ds3: DStream[((String, String), Iterable[ActionLog])] = ds2.map(log => ((log.mid, log.uid), log))
        .groupByKey()

      //过滤判断用户是否增加了收货地址
      val ds4: DStream[((String, String), Iterable[ActionLog])] = ds3.filter {
        case ((mid, uid), logs) => {

          var ifNeedAlert: Boolean = false

          Breaks.breakable {
            logs.foreach(log => {

              if (log.action_id.equals("trade_add_address")) {

                //用户指定了增加收货地址的操作，有嫌疑
                ifNeedAlert = true
                //后续无需再判断，跳出循环
                Breaks.break()

              }

            })
          }

          ifNeedAlert

        }
      }

      //判断每个设备上增加了收货地址的人数 是否 >=2,如果是就留下
      // (mid ,  [ [log,log],[log,log] ] )
      val ds5: DStream[(String, Iterable[Iterable[ActionLog]])] = ds4.map {
        case ((mid, uid), logs) => (mid, logs)
      }.groupByKey()

      //符合预警条件的设备及用户在设备上的预警日志
      val ds6: DStream[(String, Iterable[Iterable[ActionLog]])] = ds5.filter(_._2.size >= 2)

      val ds7: DStream[(String, Iterable[ActionLog])] = ds6.mapValues(_.flatten)

      //生成预警日志
      val ds8: DStream[CouponAlertInfo] = ds7.map {
        case (mid, logs) => {

          val uids: mutable.Set[String] =  new mutable.HashSet[String]
          val itemIds: mutable.Set[String] = new mutable.HashSet[String]
          val events: ListBuffer[String] = new ListBuffer[String]

          //添加预警信息
          logs.foreach(log => {

            uids.add(log.uid)

            events.append(log.action_id)

            if (log.action_id.equals("favor_add")) {

              itemIds.add(log.item)

            }

          })

          //添加ts
          val ts: Long = System.currentTimeMillis()

          /*
              添加id(体现mid)

              同一设备，如果一分钟产生多条预警，只保留最后一条预警日志

              同一分钟，一个设备的预警日志的id要相同，后续产生的才会覆盖之前产生的。

              PUT /index/type/id   {log1}
              PUT /index/type/id   {log2}

              id:mid_分钟

           */
          val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

          val minuteStr: String = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai")).format(formatter2)

          CouponAlertInfo(minuteStr + "_" + mid, uids, itemIds, events, ts)

        }
      }

      //写入ES
      import org.elasticsearch.spark._

      ds8.foreachRDD(rdd => {

        rdd.cache()

        println("当前要写入ES:"+rdd.count())

        /*
            resource : String: 要写入的index/type
             cfg : Map[String,String] : 要写入的index的配置说明
                es.mapping.id: 当前RDD中的T类型的哪个属性是作为 主键
         */
        rdd.saveToEs("realtime2022_behaviour_alert_"+LocalDate.now,Map("es.mapping.id" -> "id"))

        //提交偏移量
        ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)

      })

    }


  }
}