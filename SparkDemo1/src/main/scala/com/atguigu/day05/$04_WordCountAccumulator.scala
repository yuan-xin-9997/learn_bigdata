package com.atguigu.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.util
import scala.collection.mutable

object $04_WordCountAccumulator {
  val conf = new SparkConf().setMaster("local[4]").setAppName("test").set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  /**
   *
   * 累加器 ：
   *       使用场景：一般用于聚合，并且最终聚合结果不能太大
   *       好处：可以在一定程度上减少shuffle操作
   *       分类：longAccumulator
   *
   * 使用集合累加器collectionAccumulator处理WordCount，减少shuffle操作
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // Word Count 案例，常规做法，todo 会有shuffle操作
    val rdd1 = sc.textFile("datas/wc.txt")
    println(rdd1.getNumPartitions)
    val rdd2 = rdd1.flatMap(line => line.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map(x => (x, 1))
//    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((agg, cur) => {agg + cur})
//    println(rdd4.collect().toList)


    // ----------------------------------------------------
    // todo 下面使用集合累加器，实现没有shuffle的Word Count

    println(s"Outer Thread Name = ${Thread.currentThread().getName}")  // main

    // 获取自带的集合累加器
    val wcAcc = sc.collectionAccumulator[mutable.Map[String, Int]](name = "wcAcc")  // 存放分区所有累加结果
    val rdd5 = rdd3.foreachPartition(it => {
      println(s"rdd3.foreachPartition Thread Name = ${Thread.currentThread().getName}")  //  Thread Name = Executor task launch worker for task 0/1
      // rmap为每个分区执行word count计算的结果
      val rmap = mutable.Map[String, Int]()
      it.foreach(x => {
        // x=(hello,1)
        val num = rmap.getOrElse(x._1, 0)
        val totalNum = num + x._2
        rmap.put(x._1, totalNum)
      })
      wcAcc.add(rmap)
    })

    // 获取集合累加器结果，从Executor Task返回的Map
    // 输出数据：List(
    //    Map( Hello->5, spark->3  ),
    //    Map( Hello->5, spark->3, hadoop->1  )    // 几个分区，List就几个元素
    // )
    val res: util.List[mutable.Map[String, Int]] = wcAcc.value

    // 将java集合转成scala集合
    import scala.collection.JavaConverters._
    val scalaList: mutable.Buffer[mutable.Map[String, Int]] = res.asScala

    // 压平
    // 输出数据：List( Hello->5, spark->3, Hello->5, spark->3, hadoop->1  )
    val list: mutable.Buffer[(String, Int)] = scalaList.flatten

    // 分组
    // 输出数据：Map(
    //    Hello->List(   Hello->5, Hello->5 )
    // )
    val resMap: Map[String, mutable.Buffer[(String, Int)]] = list.groupBy(x => {
      println(s"groupBy Thread Name = ${Thread.currentThread().getName}")  // Thread Name = main
      x._1
    })

    // 计算次数
    val resFinal: Map[String, Int] = resMap.map(x => {
      println(s"map Thread Name = ${Thread.currentThread().getName}")  // Thread Name = main
      // x = Hello->List(   Hello->5, Hello->5 )
      val z = x._2.map(y => y._2)
      (x._1, z.sum)
    })

    // 打印结果
    resFinal.foreach(println)

//    Thread.sleep(1000000000)
  }
}
