package com.atguigu.day04

import org.apache.spark.{SparkConf, SparkContext}

object $06_Lineage {
  /**
   * 血统/血缘关系：从第一个RDD到当前RDD的依赖链条
   * 查看血统：rdd.toDebugString
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    println("--------------------------")
    val rdd = sc.textFile("datas/wc.txt")  // textFile生成两个RDD
    println(rdd.toDebugString)
    println("--------------------------")
    val rdd2 = rdd.flatMap(line => line.split(" "))
    println(rdd2.toDebugString)
    println("--------------------------")
    val rdd3 = rdd2.map(x => (x, 1))
    println(rdd3.toDebugString)
    println("--------------------------")
    val rdd4 = rdd3.reduceByKey(_ + _)
    println(rdd4.collect().toList)
    println(rdd4.toDebugString)
    println("--------------------------")


  }
}
