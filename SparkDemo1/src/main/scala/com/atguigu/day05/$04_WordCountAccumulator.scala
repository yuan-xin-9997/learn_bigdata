package com.atguigu.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object $04_WordCountAccumulator {
  val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  /**
   * 使用集合累加器collectionAccumulator处理WordCount，减少shuffle操作
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val rdd1 = sc.textFile("datas/wc.txt")
    println(rdd1.getNumPartitions)
    val rdd2 = rdd1.flatMap(line => line.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map(x => (x, 1))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((agg, cur) => {agg + cur})
    println(rdd4.collect().toList)

    // 获取自带的集合累加器
    val wcAcc = sc.collectionAccumulator[mutable.Map[String, Int]](name = "wcAcc")
    rdd3.foreachPartition(it=>{
      val rmap=mutable.Map[String,Int]()
      it.foreach(x=>{
        // x=(hello,1)
        val num=rmap.getOrElse(x._1, 0)

      })
    })
  }
}
