package com.atguigu.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object $06_Demo11 {
  /**
   * 第5章 Spark Core实战
   *
   * 需求说明：品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量（次数）来统计热门品类。
   * 鞋			点击数 下单数  支付数
   * 衣服		点击数 下单数  支付数
   * 电脑		点击数 下单数  支付数
   * 例如，综合排名 = 点击数*20% + 下单数*30% + 支付数*50%
   * 本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
   *
   * todo 需求：Top10热门品类
   *
   *      以下为方案一：处理时间慢，shuffle多
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    // 1. 读取数据
    val rdd1 = sc.textFile("datas/user_visit_action.txt")

    // 2. 列裁剪
    val rdd2: RDD[(String, String, String, String)] = rdd1.map(line => {
      val arr = line.split("_")
      val searchKW = arr(5)
      val clickid = arr(6)
      val orderids = arr(8)
      val payids = arr(10)
      (searchKW, clickid, orderids, payids) // 列裁剪
    })

    // 3. 过滤搜索行为数据
    val filterRDD = rdd2.filter {
      case (searchKW, clickid, orderids, payids) => searchKW == "null"
    }

    // 4. 统计每个品类的点击数
    // 4.1 过滤掉支付、下单行为数据
    val clickRDD  = filterRDD.filter {
      case (searchKW, clickid, orderids, payids) => clickid != "-1"
    }
    // 4.2 数据转换
    val trsRDD = clickRDD.map {
      case (searchKW, clickid, orderids, payids) => (clickid, 1)
    }
    // 4.3 按照点击品类id分组统计次数
    val clickNumRDD = trsRDD.reduceByKey(_ + _)

    // 5. 统计每个品类的下单数
    // 5.1 过滤掉点击、支付行为数据
    val oderRDD = filterRDD.filter {
      case (searchKW, clickid, orderids, payids) => orderids != "null"
    }
    // 5.2 数据类型转换+压平
    val orderMapRDD: RDD[(String, Int)] = oderRDD.flatMap {
      case (searchKW, clickid, orderids, payids) => {
        val ids = orderids.split(",")
        ids.map(id => (id, 1))
      }
    }
    // 5.3 统计每个品类的下单次数
    val orderNumRDD = orderMapRDD.reduceByKey(_ + _)

    // 6. 统计每个品类的支付数
    // 6.1 过滤掉点击、下单行为数据
    val payFilterRDD = filterRDD.filter {
      case (searchKW, clickid, orderids, payids) => payids != "null"
    }
    // 6.2 数据类型转换+压平
    val payMapRDD = payFilterRDD.flatMap {
      case (searchKW, clickid, orderids, payids) => {
        val ids = payids.split(",")
        ids.map(id => (id, 1))
      }
    }
    // 6.3 统计每个品类的支付次数
    val payNumRDD: RDD[(String, Int)] = payMapRDD.reduceByKey(_ + _)

    // 7. 三者join
    val clickorderRDD: RDD[(String, (Option[Int], Option[Int]))] = clickNumRDD.fullOuterJoin(orderNumRDD)
    // 7.1 处理点击次数中的Option
    val clickOrderNumRDD: RDD[(String, (Int, Int))] = clickorderRDD.map {
      case (id, (clickNum, orderNum)) => (id, (clickNum.getOrElse(0), orderNum.getOrElse(0)))
    }
    // 7.2 将clickOrderNumRDD与支付RDD 全连接
    val totalRDD: RDD[(String, (Option[(Int, Int)], Option[Int]))] = clickOrderNumRDD.fullOuterJoin(payNumRDD)
    // 7.3 处理点击次数中的Option
    val totalNumRDD = totalRDD.map {
      case (id, (clickOrderNum, payOrderNum)) => {
        val coNum = clickOrderNum.getOrElse((0, 0))
        (id, coNum._1, coNum._2, payOrderNum.getOrElse(0))
      }
    }

    // 8. 按照品类点击、下单数、支付数排序
    val rankNumRDD = totalNumRDD.sortBy ({
      case (id, clickNum, orderNum, payNum) => (clickNum, orderNum, payNum)
    }, ascending = false).take(10)

    // 9. 结果展示
    rankNumRDD.foreach(println)


    Thread.sleep(1000000)
  }
}
