package com.atguigu.day01

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.UUID

case class UserAnalysis1(userId:String,
                        actionTime:String,
                        page:String,
                        actionTimestamp:Long,
                        var session:String=UUID.randomUUID().toString,
                        var step:Int=1)

object SessionDemo1Yarn {
  /**
   * day01 用户行为轨迹分析
   * 需求：分析用户每个会话［上一次访问与本次访问是否超过半小时，如果超过则是新会话］的行为轨迹
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkContext对象
    val conf = new SparkConf().setAppName("session demo")
    val sc = new SparkContext(conf)

    // 2. 读取数据
    // RDD(
    //    "1001,2020-09-10 10:21:21,home.html",
    //    ""1001,2020-09-10 10:28:10,good_list.html:,
    //    ""1001,2020-09-10 10:35:05,good_detail.html",
    //    ""1001,2020-09-10 10:42:55,cart.html",
    //    ""1001,2020-09-10 11:35:21,home.html",
    //    ""1001,2020-09-10 11:36:10,cart.html",
    //    ""1001,2020-09-10 11:38:12,trade.html",
    //    ""1001,2020-09-10 11:38:55,payment.html",
    //    ""1002,2020-09-10 09:40:00,home.html",
    //    ""1002,2020-09-10 09:41:00,mine.html",
    //    ""1002,2020-09-10 09:42:00,favor.html",
    //    ""1003,2020-09-10 13:10:00,home.html",
    //    "1003,2020-09-10 13:15:00,search.html",
    // )
    val rdd1 = sc.textFile(args(0))
    rdd1.collect()
    rdd1.foreach(println)

    // 3.转换数据
    // RDD(
    //    UserAnalysis(1001,2020-09-10 10:21:21,home.html,sessionid,uuid,1)
    //    UserAnalysis(1001,2020-09-10 10:28:10,good_list.html,sessionid,uuid,1)
    //    UserAnalysis(1001,2020-09-10 10:35:05,good_detail.html,sessionid,uuid,1)
    //    UserAnalysis(1001,2020-09-10 10:42:55,cart.html,sessionid,uuid,1)
    //    UserAnalysis(1001,2020-09-10 11:35:21,home.html,sessionid,uuid,1)
    //    UserAnalysis(1001,2020-09-10 11:36:10,cart.html,sessionid,uuid,1)
    //    UserAnalysis(1001,2020-09-10 11:38:12,trade.html,sessionid,uuid,1)
    //    UserAnalysis(1001,2020-09-10 11:38:55,payment.html,sessionid,uuid,1)
    //    UserAnalysis(1002,2020-09-10 09:40:00,home.html,sessionid,uuid,1)
    //    UserAnalysis(1002,2020-09-10 09:41:00,mine.html,sessionid,uuid,1)
    //    UserAnalysis(1002,2020-09-10 09:42:00,favor.html,sessionid,uuid,1)
    //    UserAnalysis(1003,2020-09-10 13:10:00,home.html,sessionid,uuid,1)
    //    UserAnalysis(1003,2020-09-10 13:15:00,search.html,sessionid,uuid,1)
    // )
    val rdd2 = rdd1.map(
      line => {
        val arr = line.split(",")
        val userid = arr.head
        val actionTime = arr(1)
        val page = arr(2)
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val ts = format.parse(actionTime).getTime

        UserAnalysis1(userid, actionTime, page, ts)
      }
    )
    rdd2.collect()
    rdd2.foreach(println)

    // 4. 按照用户分组
    // RDD(
    //    1001->Iterable(
    //        UserAnalysis(1001,2020-09-10 10:21:21,home.html,sessionid,uuid,1)
    //        UserAnalysis(1001,2020-09-10 10:28:10,good_list.html,sessionid,uuid,1)
    //        UserAnalysis(1001,2020-09-10 10:35:05,good_detail.html,sessionid,uuid,1)
    //        UserAnalysis(1001,2020-09-10 10:42:55,cart.html,sessionid,uuid,1)
    //        UserAnalysis(1001,2020-09-10 11:35:21,home.html,sessionid,uuid,1)
    //        UserAnalysis(1001,2020-09-10 11:36:10,cart.html,sessionid,uuid,1)
    //        UserAnalysis(1001,2020-09-10 11:38:12,trade.html,sessionid,uuid,1)
    //        UserAnalysis(1001,2020-09-10 11:38:55,payment.html,sessionid,uuid,1)
    //    ),
    //    1002->Iterable(
    //        UserAnalysis(1002,2020-09-10 09:40:00,home.html,sessionid,uuid,1)
    //        UserAnalysis(1002,2020-09-10 09:41:00,mine.html,sessionid,uuid,1)
    //        UserAnalysis(1002,2020-09-10 09:42:00,favor.html,sessionid,uuid,1)
    //    ),
    //    1003->Iterable(
    //        UserAnalysis(1003,2020-09-10 13:10:00,home.html,sessionid,uuid,1)
    //        UserAnalysis(1003,2020-09-10 13:15:00,search.html,sessionid,uuid,1)
    //    ),
    //    ...
    // )
    val rdd3 = rdd2.groupBy(
      us => us.userId
    )
    rdd3.collect()
    rdd3.foreach(println)

    // 5. 针对每个用户，按照时间排序，滑窗，计算是否属于同一个session
    val rdd4 = rdd3.flatMap(
      // x = 1001 -> Iterable(
      //        UserAnalysis(1001,2020-09-10 10:21:21,home.html,sessionid,uuid,1)
      //        UserAnalysis(1001,2020-09-10 10:28:10,good_list.html,sessionid,uuid,1)
      //        UserAnalysis(1001,2020-09-10 10:35:05,good_detail.html,sessionid,uuid,1)
      //        UserAnalysis(1001,2020-09-10 10:42:55,cart.html,sessionid,uuid,1)
      //        UserAnalysis(1001,2020-09-10 11:35:21,home.html,sessionid,uuid,1)
      //        UserAnalysis(1001,2020-09-10 11:36:10,cart.html,sessionid,uuid,1)
      //        UserAnalysis(1001,2020-09-10 11:38:12,trade.html,sessionid,uuid,1)
      //        UserAnalysis(1001,2020-09-10 11:38:55,payment.html,sessionid,uuid,1)
      //    )
      x => {
        // 针对用户数据排序
        val sortedUserList = x._2.toList.sortBy(y => y.actionTimestamp)


        // 滑窗得到上一条和下一条数据
        val slidingList = sortedUserList.sliding(2)
        // Iterable(
        //   (UserAnalysis(1001,2020-09-10 10:21:21,home.html,sessionid,uuid,1), UserAnalysis(1001,2020-09-10 10:28:10,good_list.html,sessionid,uuid,1)),
        //   (UserAnalysis(1001,2020-09-10 10:28:10,good_list.html,sessionid,uuid,1), UserAnalysis(1001,2020-09-10 10:35:05,good_detail.html,sessionid,uuid,1)),
        //   (UserAnalysis(1001,2020-09-10 10:35:05,good_detail.html,sessionid,uuid,1), UserAnalysis(1001,2020-09-10 10:42:55,cart.html,sessionid,uuid,1)),
        //   ...
        // )

        // 上一次与本次访问时间是否小于半小时，如果小，则属于同一个会话
        slidingList.foreach(y => {
          // y =(UserAnalysis(1001,2020-09-10 10:21:21,home.html,sessionid,uuid,1), UserAnalysis(1001,2020-09-10 10:28:10,good_list.html,sessionid,uuid,1))
          val beforeTime = y.head.actionTimestamp
          val afterTime = y.last.actionTimestamp
          if (afterTime - beforeTime <= 30 * 60 * 1000) {
            y.last.session = y.head.session
            y.last.step = y.head.step + 1
          }
        })

        x._2
      }
    )

    // 6. 结果展示
    val arr = rdd4.collect()
    arr.foreach(println)
  }
}