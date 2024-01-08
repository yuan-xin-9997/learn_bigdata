package com.atguigu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
 * transformation算子案例
 * 案例实操（省份广告被点击Top3）
 *
 *
 */
class $02_Demo {
  val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  @Test
  def Plan1(): Unit = {
    // 1. 读取文件
    val rdd1: RDD[String] = sc.textFile("datas/agent.log")
//    println(rdd1.collect().toList)

    // 2. 列裁剪【省份，广告ID】
    // 输出数据：RDD(
    //  (广东省,A)
    //  (广东省,B)
    //  (广东省,C)
    //  ....
    // )
    val rdd2: RDD[(String, String)] = rdd1.map(line => {
      val arr = line.split(" ")
      val provinceID = arr(1)
      val adid = arr.last
      (provinceID, adid)
    })
    println(rdd2.collect().toList)
    println("--------------")

    // 3. 按照省份分组
    // 输出数据：RDD(
    //    广东省->Iterable(A,B,C,D,E,F),
    //    ...
    // )
//    val rdd3 = rdd2.groupBy(x => x._1)
    // todo 按照省份分组，这一步会出现数据倾斜，对省份key进行取模Hash运算结果，最多是33个分区，如
    //  果分区个数100个，那么只有33个分区有数据，其他67个分区无数据
    val rdd3: RDD[(String, Iterable[String])] = rdd2.groupByKey()
    println(rdd3.collect().toList)
    println(rdd3.getNumPartitions)
    println("--------------")

    // 4. 统计每个省份中每个广告的点击次数
    // 输出数据：RDD(
    //    广东省->Map( 广东省->List(A-33,B->22,C->11),...    ),
    //    ...
    // )
    val rdd4: RDD[(String, List[(String, Int)])] = rdd3.map(x => {
      //val dict = x._2.toList.map(y => (y, 1))
      //dict.reduce()

      // 对每个省份内的按广告名分组
      // 输出数据：Map(
      //   A->Iterable(A,B,C,D,E,F),
      //   B->Iterable(A,B,C,D,E,F)
      // )
      val groupedMap: Map[String, Iterable[String]] = x._2.groupBy(y => y)

      // 对每个省份内，每个广告点击次数求和
      // 输出数据：Map(A->30,B->20,C->40,...)
      val numMap: Map[String, Int] = groupedMap.map(y => (y._1, y._2.size))

      // 5. 对每个省份所有广告按照点击次数排序取前三
      val top3: List[(String, Int)] = numMap.toList.sortBy(y => y._2).reverse.take(3)

      (x._1, top3)
    })
    //println(rdd4.collect().toList)
    rdd4.foreach(println)
    println("--------------")
  }

  @Test
  def Plan2(): Unit = {
    // 1. 读取文件
    val rdd1: RDD[String] = sc.textFile("datas/agent.log")
    //    println(rdd1.collect().toList)

    // 2. 列裁剪【省份，广告ID】+转换数据格式
    // 输出数据：RDD(
    //  (广东省,A)->1,
    //  (广东省,B)->1,
    //  (广东省,C)->1,
    //  ....
    // )
    val rdd2: RDD[((String, String), Int)] = rdd1.map(line => {
      val arr = line.split(" ")
      val provinceID = arr(1)
      val adid = arr.last
      (provinceID, adid)->1
    })
    println(rdd2.collect().toList)
    println("--------------")

    // 3. 按照省份+id，统计每个省份每个广告的点击次数
    // todo 采用这种方式可以减少数据倾斜，如何倾斜？原因待深究
    // 输出数据：RDD(
    //    (广东省,A)->15,
    //    (广东省,B)->13,
    //    (广东省,C)->12,
    //    (江西省,C)->12,
    //    ...
    // )
    // reduceByKey(func:(value值类型, value值类型)=>value值类型): 对Key分组，对Value值聚合
    val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey((agg, curr) => agg + curr)
    println(rdd3.collect().toList)
    println("--------------")

    // 4. 按照省份分组
    // 输出数据  RDD(
    //    广东省->Iterable( (广东省,A)->15, (广东省,B)->16... )
    // )
    val rdd4: RDD[(String, Iterable[((String, String), Int)])] = rdd3.groupBy {
      case ((province, adid), point) => province
    }
    println(rdd4.collect().toList)
    println("--------------")

    // 5. 对每个省份所有广告按照点击次数排序取前三
    val rdd5 = rdd4.map(x => {
      // 输入数据：x = 广东省->Iterable( (广东省,A)->15, (广东省,B)->16... )
      val top3: List[((String, String), Int)] = x._2.toList.sortBy(y => y._2).reverse.take(3)

      // 输入数据： 广东省->Iterable( (广东省,A)->15, (广东省,B)->16... )  已排好序
      val t3: List[(String, Int)] = top3.map{
        case ((province, adid), point) => (adid, point)
      }
      (x._1, t3)
    })

    // 6. 结果展示
    rdd5.collect().foreach(println)
    println("--------------")
  }
}
