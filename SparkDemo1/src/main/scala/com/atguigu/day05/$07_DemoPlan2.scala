package com.atguigu.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object $07_DemoPlan2 {
  /**
   * 第5章 Spark Core实战
   * 需求说明：品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量（次数）来统计热门品类。
   * 鞋			点击数 下单数  支付数
   * 衣服		点击数 下单数  支付数
   * 电脑		点击数 下单数  支付数
   * 例如，综合排名 = 点击数*20% + 下单数*30% + 支付数*50%
   * 本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
   * todo 需求：Top10热门品类
   *      以下为方案二： 还有2次shuffle
   *
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

    // 3. 炸开下单ids、支付ids，并转换数据类型 (品类id， (点击标识，下单标识，支付标识))
    val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.flatMap {
      case (searchKW, clickid, orderids, payids) => {
        if (clickid != "-1") {
          // todo 此处如果不加上 :: Nil，则会报错,          ?????????此处需要是List，否则炸裂的时候会报错??????????
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
        else {
          val tuples1: Array[(String, (Int, Int, Int))] = payids.split(",").map(id => (id, (0, 0, 1)))
          tuples1}
      }
    }

    // 4. 按照品类id分组聚合，统计点击次数、下单次数、支付次数
    val rdd4: RDD[(String, (Int, Int, Int))] = rdd3.reduceByKey((agg, curr) => (agg._1 + curr._1, agg._2 + curr._2, agg._3 + curr._3))

    // 5. 排序，取前10
    val result: Array[(String, (Int, Int, Int))] = rdd4.sortBy(x => x._2, ascending = false).take(10)

    result.foreach(println)

    Thread.sleep(1000000)
  }
}
