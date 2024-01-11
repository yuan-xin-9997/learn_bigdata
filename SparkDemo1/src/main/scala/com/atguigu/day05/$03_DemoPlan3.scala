package com.atguigu.day05

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.util
import scala.collection.mutable

object $03_DemoPlan3 {
  /**
   * 第5章 Spark Core实战
   * 需求说明：品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量（次数）来统计热门品类。
   * 鞋			点击数 下单数  支付数
   * 衣服		点击数 下单数  支付数
   * 电脑		点击数 下单数  支付数
   * 例如，综合排名 = 点击数*20% + 下单数*30% + 支付数*50%
   * 本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
   * todo 需求：Top10热门品类
   * 以下为方案三：
   *      可以实现需求，使用了集合累加器之后，已经不存在shuffle了
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    // 1. 读取数据
    val rdd1 = sc.textFile("datas/user_visit_action.txt")
    println(rdd1.getNumPartitions)

    // 2. 列裁剪
    val rdd2: RDD[(String, String, String, String)] = rdd1.map(line => {
      val arr = line.split("_")
      val searchKW = arr(5)
      val clickid = arr(6)
      val orderids = arr(8)
      val payids = arr(10)
      (searchKW, clickid, orderids, payids) // 列裁剪
    })

    // 3. 炸开下单ids、支付ids，并转换数据类型 (品类id， (点击标识，下单标识，支付标识))
    val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.flatMap {
      case (searchKW, clickid, orderids, payids) => {
        if (clickid != "-1") {
          // todo 此处如果不加上 :: Nil，则会报错
          //    Type mismatch. Required: TraversableOnce[(String, (Int, Int, Int))], found: (String, (Int, Int, Int))
          //    Nil表示空集合
          //    不加上::Nil，直接(clickid, (1, 0, 0))表示是一个元组，即2个元素的元素Tuple2，元组一旦创建，元素个数、元素都不可变
          val tuples: List[(String, (Int, Int, Int))] = (clickid, (1, 0, 0)) :: Nil
          tuples
        }
        else if (orderids != "null") {
          val tuples: Array[(String, (Int, Int, Int))] = orderids.split(",").map(id => (id, (0, 1, 0)))
          tuples
        }
        else
          payids.split(",").map(id => (id, (0, 0, 1)))
      }
    }

    // todo 4.使用Spark集合累加器
    val top10Acc = sc.collectionAccumulator[mutable.Map[String, (Int, Int, Int)]]("top10Acc")

    // 针对每个分区进行每个品类点击总数、下单总数、支付总数的计算
    val rdd4 = rdd3.foreachPartition(it => {
      println(s"foreachPartition thread=${Thread.currentThread().getName}")
      // 构造空Map，结构与集合累加器相同
      val rMap = mutable.Map[String, (Int, Int, Int)]()
      // 遍历分区数据，对每个分区品类进行累加
      it.foreach {
        case (id, (clickflag, orderflag, payflag)) =>
          val beforeNum = rMap.getOrElse(id, (0, 0, 0))
          val clickNum = beforeNum._1 + clickflag
          val orderNum = beforeNum._2 + orderflag
          val payNum = beforeNum._3 + payflag
          rMap.put(id, (clickNum, orderNum, payNum))
      }
      // 将当前分区的累加结果添加到分区累加器
      top10Acc.add(rMap)
    })

    // 取出累加器结果，元素是每个分区的累加Map结果
    val javaList: util.List[mutable.Map[String, (Int, Int, Int)]] = top10Acc.value

    // 将JavaList转换为ScalaList
    import scala.collection.JavaConverters._
    // scalaList = Buffer(
    //      元素共2个，=分区数量
    //      Map(2 -> (3073,865,600), 5 -> (3006,880,555), 12 -> (2994,859,591), 8 -> (2955,854,617), 15 -> (3039,826,619), 18 -> (3043,887,602), 7 -> (3003,867,653), 1 -> (2952,904,589), 17 -> (3047,873,616), 4 -> (2993,867,612), 11 -> (3046,890,577), 14 -> (3043,909,597), 20 -> (2975,873,621), null -> (0,0,20153), 6 -> (2948,856,596), 9 -> (3109,892,602), 16 -> (3004,881,611), 19 -> (3013,872,571), 3 -> (2972,860,592), 10 -> (3028,883,564), 13 -> (3026,921,599))
    //      Map(2 -> (3046,902,596), 5 -> (3005,940,577), 12 -> (3101,881,627), 15 -> (3081,846,640), 8 -> (3019,882,621), 18 -> (2981,867,595), 7 -> (3071,929,599), 17 -> (3032,879,615), 1 -> (3024,862,602), 4 -> (2968,893,659), 11 -> (3047,891,625), 14 -> (2921,864,574), 20 -> (3123,903,623), null -> (0,0,20079), 6 -> (2964,912,601), 9 -> (2936,844,628), 16 -> (2924,901,622), 19 -> (3031,850,587), 10 -> (2963,874,610), 3 -> (3003,889,600), 13 -> (3010,860,562))
    // )
    val scalaList: mutable.Buffer[mutable.Map[String, (Int, Int, Int)]] = javaList.asScala

    // 压平
    // rdd5List = Buffer(
    //      2 -> (3073,865,600), 5 -> (3006,880,555), 12 -> (2994,859,591), 8 -> (2955,854,617), 15 -> (3039,826,619), 18 -> (3043,887,602), 7 -> (3003,867,653), 1 -> (2952,904,589), 17 -> (3047,873,616), 4 -> (2993,867,612), 11 -> (3046,890,577), 14 -> (3043,909,597), 20 -> (2975,873,621), null -> (0,0,20153), 6 -> (2948,856,596), 9 -> (3109,892,602), 16 -> (3004,881,611), 19 -> (3013,872,571), 3 -> (2972,860,592), 10 -> (3028,883,564), 13 -> (3026,921,599)
    //      2 -> (3046,902,596), 5 -> (3005,940,577), 12 -> (3101,881,627), 15 -> (3081,846,640), 8 -> (3019,882,621), 18 -> (2981,867,595), 7 -> (3071,929,599), 17 -> (3032,879,615), 1 -> (3024,862,602), 4 -> (2968,893,659), 11 -> (3047,891,625), 14 -> (2921,864,574), 20 -> (3123,903,623), null -> (0,0,20079), 6 -> (2964,912,601), 9 -> (2936,844,628), 16 -> (2924,901,622), 19 -> (3031,850,587), 10 -> (2963,874,610), 3 -> (3003,889,600), 13 -> (3010,860,562)
    // )
    val flattenScalaList: mutable.Buffer[(String, (Int, Int, Int))] = scalaList.flatten
    println(flattenScalaList)

    // 分组
    // groupedMap = Map(
    //   2->Buffer( 2 -> (3073,865,600), 2 -> (3046,902,596)  ),
    //   ...
    // )
    val groupedMap: Map[String, mutable.Buffer[(String, (Int, Int, Int))]] = flattenScalaList.groupBy(x => x._1)

    // 计算每个品类点击总数、下单总数、支付总数
    val numMap: Map[String, (Int, Int, Int)] = groupedMap.map(x => {
      // x=  2->Buffer( 2 -> (3073,865,600), 2 -> (3046,902,596)  )
      val clickNum = x._2.map(y => y._2._1).sum
      val orderNum = x._2.map(y => y._2._2).sum
      val payNum = x._2.map(y => y._2._3).sum
      (x._1, (clickNum, orderNum, payNum))
    })

    // 排序
    val result: List[(String, (Int, Int, Int))] = numMap.toList.sortBy(x => x._2).reverse.take(10)

    // 输出结果
    result.foreach(println)

    Thread.sleep(1000000)

  }
}
