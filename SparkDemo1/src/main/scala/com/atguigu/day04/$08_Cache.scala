package com.atguigu.day04

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object $08_Cache {
  /**
   * RDD持久化使用场景：
   *      1、一个RDD在多个job中重复使用
   *          问题：每个job执行的时候，该RDD之前的处理步骤，也会重复执行
   *          使用持久化好处：可以只计算一次，不用重复计算
   *      2、如果一个job血统/依赖链条很长
   *          问题：如果其中一环数据丢失，需要重新计算，会浪费大量时间
   *          使用持久化好处：可以直接将持久化数据拿来计算，不用重头计算，节省时间
   * RDD持久化方式
   *      1、缓存
   *          数据保存位置：分区所在的主机的内存/磁盘中
   *          数据报错时机：在cache rdd所在第一个job执行过程中保存
   *          用法：rdd.cache() / rdd.persist()
   *                cache()与persist()区别
   *                cache只将数据保存在内存中，底层def cache(): this.type = persist()
   *                persist可以指定将数据保存在内存/磁盘/堆外中
   *                工作中常用存储级别：
   *                        StorageLevel.MEMORY_ONLY，数据只保存在内存中，一般用于小数据量场景
   *                        StorageLevel.MEMORY_AND_DISK，数据同时保存在内存和磁盘中，一般用于大数据量场景
   *      2、checkpoint
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("datas/wc.txt")
    val rdd2 = rdd.flatMap(line => {
      println("数数我打印了多少次")
      line.split(" ")
    })

    //rdd2.cache()    // 底层 def cache(): this.type = persist()
    rdd2.persist(StorageLevel.MEMORY_AND_DISK)

    // TODO 如果没有持久化 下面 计算， rdd2会被调用两次
    val rdd3 = rdd2.map(x => (x, 1))
    val rdd4 = rdd2.map(x => x.size)

    println(rdd3.collect().toList)
    println(rdd4.collect().toList)

    Thread.sleep(1000000)

  }
}
