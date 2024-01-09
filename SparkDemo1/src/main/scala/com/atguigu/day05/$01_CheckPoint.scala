package com.atguigu.day05

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object $01_CheckPoint {
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
   *          数据保存位置：分区所在的 todo 主机的内存/磁盘中
   *          数据报错时机：在cache rdd所在第一个job执行过程中保存
   *          用法：rdd.cache() / rdd.persist()
   *                cache()与persist()区别
   *                cache只将数据保存在内存中，底层def cache(): this.type = persist()
   *                persist可以指定将数据保存在内存/磁盘/堆外中
   *                工作中常用存储级别：
   *                        StorageLevel.MEMORY_ONLY，数据只保存在内存中，一般用于小数据量场景
   *                        StorageLevel.MEMORY_AND_DISK，数据同时保存在内存和磁盘中，一般用于大数据量场景
   *      2、checkpoint
   *            出现checkpoint的原因：缓存是将数据保存在分区所在的主机内存/磁盘中，如果所在服务器宕机，数据丢失，需要重新计算
   *            数据保存位置：HDFS
   *            数据保存时机：在checkpoint rdd所在第一个job执行完成之后，会再次启动一个job将执行到rdd为止得到的数据进行保存
   *            使用方式：
   *                1. 指定保存数据路径：sc.setCheckpointDir(path)
   *                2. 持久rdd数据：rdd.checkpoint
   *            checkpoint会单独触发一次job执行，得到checkpoint rdd进行数据保存。所以可以结合缓存一起使用，
   *                避免checkpoint触发的job重复处理数据
   * 缓存与checkpoint区别：
   *      1. 数据保存位置不一样，
   *           缓存是将数据保存在分区所在服务机的内存/磁盘中
   *           checkpoint可以将数据保存到HDFS中
   *      2. 数据保存时机不一样，
   *           缓存是在cache rdd所在第一个job执行过程中保存数据
   *           checkpoint是在checkpoint rdd所在第一个job执行完成之后，会再次启动一个job执行到checkpoint rdd为止得到的数据进行保存
   *      3. 依赖关系是否保留不一样
   *            缓存是将数据保存在分区所在主机的内存/磁盘中，数据可能丢失，丢失之后需要根据RDD依赖关系重新计算得到数据，所以RDD的依赖关系会保留
   *            checkpoint是将数据保存到HDFS中，数据不会丢失（理想情况下），所以checkpoint rdd 之前的依赖会删除
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("datas/checkpoint")

    val rdd = sc.textFile("datas/wc.txt")
    val rdd2 = rdd.flatMap(line => {
      println("数数我打印了多少次")
      line.split(" ")
    })

    // rdd2.cache()    // 底层 def cache(): this.type = persist()
    // rdd2.persist(StorageLevel.MEMORY_AND_DISK)
    rdd2.checkpoint()
    println("-----运行前血统-----")
    println(rdd2.toDebugString)

    // TODO 如果没有持久化 下面 计算， rdd2会被调用两次
    val rdd3 = rdd2.map(x => (x, 1))
    val rdd4 = rdd2.map(x => x.size)

    println(rdd3.collect().toList)
    println("-----运行后血统-----")
    println(rdd2.toDebugString)

    //println(rdd4.collect().toList)
    //println(rdd4.collect().toList)
    println(rdd4.collect().toList)

    // Thread.sleep(1000000)

  }
}
