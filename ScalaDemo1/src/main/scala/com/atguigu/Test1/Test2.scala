package com.atguigu.Test1

import java.text.SimpleDateFormat
import scala.io.Source

object Test2 {
  /**
   * 案例2：统计每个区域的平均等客时间【使用 hive sql+scala分别实现】
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 读取数据
    val datas = Source.fromFile("datas/taxi.txt").getLines().toList
    println("========origin data========")
    println(datas)
    println(datas.size)

    // 1.列裁剪
    // List(
    //    A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02
    //    B,福田区,2020-07-15 11:43:22,2020-07-15 11:55:45
    //    A,宝安区,2020-07-15 11:55:55,2020-07-15 12:12:23
    //    B,宝安区,2020-07-15 12:05:05,2020-07-15 12:22:33
    //    A,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15
    //    A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50
    //    B,龙岗区,2020-07-15 10:45:25,2020-07-15 10:50:00
    //    A,龙岗区,2020-07-15 11:33:12,2020-07-15 11:45:35
    //    B,龙岗区,2020-07-15 12:27:20,2020-07-15 12:43:31
    //    A,龙岗区,2020-07-15 12:17:10,2020-07-15 12:33:21
    //    B,龙华区,2020-07-15 10:15:21,2020-07-15 10:35:12
    //    B,宝安区,2020-07-15 11:12:18,2020-07-15 11:27:25
    // )
    val selectList = datas.map(line => {
      val arr = line.split(",")
      val userID = arr.head
      val toRegion = arr(2)
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val fromTime = formatter.parse(arr(3)).getTime
      val toTime = formatter.parse(arr(4)).getTime
      (userID, toRegion, fromTime, toTime)
    })
    println("========selectList========")
    println(selectList)
    println(selectList.size)


    // 2. 按照用户分组
    // Map(
    //  B->List(
    //            B,宝安区,2020-07-15 12:05:05,2020-07-15 12:22:33
    //            B,福田区,2020-07-15 11:43:22,2020-07-15 11:55:45
    //            B,龙岗区,2020-07-15 10:45:25,2020-07-15 10:50:00
    //            B,龙岗区,2020-07-15 12:27:20,2020-07-15 12:43:31
    //            B,龙华区,2020-07-15 10:15:21,2020-07-15 10:35:12
    //            B,宝安区,2020-07-15 11:12:18,2020-07-15 11:27:25
    //  ),
    //
    //  A->List(
    //        A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02
    //        A,宝安区,2020-07-15 11:55:55,2020-07-15 12:12:23
    //        A,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15
    //        A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50
    //        A,龙岗区,2020-07-15 11:33:12,2020-07-15 11:45:35
    //        A,龙岗区,2020-07-15 12:17:10,2020-07-15 12:33:21
    //     )
    //
    // )
    //
    println("========groupMap========")
    val groupedMap = selectList.groupBy(x => x._1)
    println(groupedMap)

    // 3. 对每个用户所有数据按照时间进行排序
    // Map(
    //  B->List(
    //            B,宝安区,2020-07-15 11:12:18,2020-07-15 11:27:25
    //            B,宝安区,2020-07-15 12:05:05,2020-07-15 12:22:33
    //            B,福田区,2020-07-15 11:43:22,2020-07-15 11:55:45
    //            B,龙岗区,2020-07-15 10:45:25,2020-07-15 10:50:00
    //            B,龙岗区,2020-07-15 12:27:20,2020-07-15 12:43:31
    //            B,龙华区,2020-07-15 10:15:21,2020-07-15 10:35:12
    //  ),
    //
    //  A->List(
    //        A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02
    //        A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50
    //        A,宝安区,2020-07-15 11:55:55,2020-07-15 12:12:23
    //        A,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15
    //        A,龙岗区,2020-07-15 11:33:12,2020-07-15 11:45:35
    //        A,龙岗区,2020-07-15 12:17:10,2020-07-15 12:33:21
    //     )
    //
    // )
    println("========sortedMap========")
    val sortedMap = groupedMap.map(x => {
      val sortedList = x._2.sortBy(y => y._3)
      (x._1, sortedList)
    })
    println(sortedMap)
    sortedMap.foreach(println)
    println(sortedMap.size)

    // 4. 对用户数据滑窗
    //List(
    //   List( (A,宝安区,2020-07-15 10:05:10,2020-07-15 10:25:02), (A,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50) ),
    //   List( (B,龙岗区,2020-07-15 10:35:15,2020-07-15 10:40:50), (B,龙华区,2020-07-15 11:02:08,2020-07-15 11:17:15)),
    //  ......
    // )
    println("========before slidingList========")
    sortedMap.toList.foreach(println)
    println(sortedMap.toList)
    println("========after slidingList========")
    val slidingList = sortedMap.toList.map(x => {
      val list = x._2.sliding(2).toList
      list
    }).flatten
    println(slidingList)
    slidingList.foreach(println)

    //5、计算每次等客时间和区域
    println("================")
    val durationList = slidingList.map(x => {
      val region = x.head._2
      val toTime = x.head._4
      val fromTime = x.last._3
      // 等客时间
      val duration = (fromTime - toTime).toDouble / 1000
      (region, duration)
    })
    println(durationList)
    println(durationList.size)
    durationList.foreach(println)

    //6、按照区域分组
    println("================")
    val regionMap = durationList.groupBy(x => x._1)
    println(regionMap)
    println(regionMap.size)
    regionMap.foreach(println)

    //7、计算每个区域的平均等客时间
    println("================")
    val result = regionMap.map(x => {
      val region = x._1
      val count = x._2.size
      val totalTime = x._2.map(y => y._2).sum
      (region, totalTime / count)
    })

    // 8. 打印结果
    println("================")
    println(result)
    result.foreach(println)
  }
}
