package com.atguigu.Test1.official

import java.text.SimpleDateFormat

import scala.io.Source

object Test2 {

  def main(args: Array[String]): Unit = {

    //读取数据
    val datas = Source.fromFile("datas/taxi.txt").getLines().toList

    //1、列裁剪[用户id,下车区域,上车时间,下车时间]
    val selectList = datas.map(line=>{
      val arr = line.split(",")
      val userId = arr.head
      val toRegion = arr(2)
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val fromTime = formatter.parse(arr(3)).getTime
      val toTime = formatter.parse(arr(4)).getTime
      (userId,toRegion,fromTime,toTime)
    })
    //List(
    //  (A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02),
    //  (B,福田区,2020-07-15 11:43:22,2020-07-15 11:55:45),
    //  (A,宝安区,2020-07-15 11:55:55,2020-07-15 12:12:23),
    //  (B,宝安区,2020-07-15 12:05:05,2020-07-15 12:22:33),
    //  (A,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15),
    //  (A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50),
    //  (B,龙岗区,2020-07-15 10:45:25,2020-07-15 10:50:00),
    //  (A,龙岗区,2020-07-15 11:33:12,2020-07-15 11:45:35),
    //  (B,龙岗区,2020-07-15 12:27:20,2020-07-15 12:43:31),
    //  (A,龙岗区,2020-07-15 12:17:10,2020-07-15 12:33:21),
    //  (B,龙华区,2020-07-15 10:15:21,2020-07-15 10:35:12),
    //  (B,宝安区,2020-07-15 11:12:18,2020-07-15 11:27:25)
    // )

    //2、按照用户分组
    val groupedMap = selectList.groupBy(x=>x._1)
    //Map(
    //    A -> List(
    //        (A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02),
    //        (A,宝安区,2020-07-15 11:55:55,2020-07-15 12:12:23),
    //        (A,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15),
    //        (A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50),
    //        (A,龙岗区,2020-07-15 11:33:12,2020-07-15 11:45:35),
    //        (A,龙岗区,2020-07-15 12:17:10,2020-07-15 12:33:21),
    //     ),
    //   B -> List(
    //      (B,福田区,2020-07-15 11:43:22,2020-07-15 11:55:45),
    //      (B,宝安区,2020-07-15 12:05:05,2020-07-15 12:22:33),
    //      (B,龙岗区,2020-07-15 10:45:25,2020-07-15 10:50:00),
    //      (B,龙岗区,2020-07-15 12:27:20,2020-07-15 12:43:31),
    //      (B,龙华区,2020-07-15 10:15:21,2020-07-15 10:35:12),
    //      (B,宝安区,2020-07-15 11:12:18,2020-07-15 11:27:25)
    //    )
    // )

    //3、对每个用户所有数据按照时间排序
    val userList = groupedMap.toList.flatMap(x=>{
      //x =    A -> List(
      //        (A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02),
      //        (A,宝安区,2020-07-15 11:55:55,2020-07-15 12:12:23),
      //        (A,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15),
      //        (A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50),
      //        (A,龙岗区,2020-07-15 11:33:12,2020-07-15 11:45:35),
      //        (A,龙岗区,2020-07-15 12:17:10,2020-07-15 12:33:21),
      //   )
      val sortedList = x._2.sortBy(y=> y._3)
      //List(
      //             (A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02),
      //             (A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50),
      //             (A,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15),
      //             (A,龙岗区,2020-07-15 11:33:12,2020-07-15 11:45:35),
      //             (A,宝安区,2020-07-15 11:55:55,2020-07-15 12:12:23),
      //             (A,龙岗区,2020-07-15 12:17:10,2020-07-15 12:33:21),
      // )
      val it = sortedList.sliding(2).toList
      //4、对用户数据滑窗
      //List(
      //   List( (A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02), (A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50) ),
      //   List( (A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50), (A,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15)),
      //  ......
      // )
    //5、计算每次等客时间和区域
      val list = it.map(y=>{
        //y = List( (A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02), (A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50) )
        val region = y.head._2
        val toTime = y.head._4
        val fromTime = y.last._3
        //等客时间
        val duration = (fromTime-toTime).toDouble / 1000
        (region, duration)
      })
    //List( 宝安区 -> 14, 龙华区->20, 龙岗区->25, 宝安区->25 ,...)

      list
    })

    //6、按照区域分组
    val durationMap = userList.groupBy(x=>x._1)
    //Map(
    //   宝安区 -> List( 宝安区 -> 14, 宝安区->25 )
    //   龙华区 -> List( 龙华区 -> 20, 龙华区->... )
    //   ....
    // )

    //7、计算每个区域的平均等客时间
    val result = durationMap.map(x=>{
      //x =  宝安区 -> List( 宝安区 -> 14, 宝安区->25 )
        val sumTime = x._2.map(y=>y._2).sum
        val count = x._2.size
      (x._1, sumTime/count)
    })
    //8、结果打印
    result.foreach(println)

  }
}