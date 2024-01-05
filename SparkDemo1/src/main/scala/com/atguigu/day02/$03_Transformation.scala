package com.atguigu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class $03_Transformation {

  val conf = new SparkConf()
    .setMaster("local[*]").setAppName("test")
    .set("spark.testing.memory", "2147480000")
    .set("spark.default.parallelism", "10")
  val sc = new SparkContext(conf)
  /**
   * Spark算子分为两类：
   *    1. transformation转换算子：会生成新的RDD，不会出发任务计算
   *    2. action行动算子：不会生成RDD，会触发任务计算
   * @param args
   */
//  def main(args: Array[String]): Unit = {
//
//  }

  /**
   * mop(func:RDD元素类型=>B):一对一映射【原RDD元素经过映射得到新RDD的一个元素】
   *      map里面的func函数是针对RDD每个元素进行操作，函数执行次数=元素个数
   *      map生成新RDD元素个数=原RDD元素个数
   *      map使用场景：用于数据类型/值的转换（一对一）
   */
  @Test
  def map(): Unit = {
    val list: List[Int] = List(1,4,3,6,9,10)
    val rdd: RDD[Int] = sc.parallelize(list, 3)

    println(rdd.getNumPartitions)

    val rdd2 = rdd.map(x => {
      println(s"${Thread.currentThread().getName} -- ${x}")
      x * 10
    })
    println(rdd2.collect().toList)
  }

  /**
   * flatMap(func:RDD元素类型=>集合) = map+flatten   转换+压平
   *       flatmap里面的func函数是针对RDD每个元素进行操作，函数执行次数=元素个数
   *       flatmap生成新RDD元素个数一般>=原RDD元素个数
   *       faltmap使用场景：一对多
   */
  @Test
  def flatMap(): Unit = {
    val list = List("hello java", "hello hadooop", "hello spark")
    val rdd = sc.makeRDD(list)
    val rdd2 = rdd.flatMap(x => {
      println(s"${Thread.currentThread().getName} -- ${x}")
      x.split(" ")
    })
    println(rdd2.collect().toList)
  }

  /**
   * 过滤，按照指定条件过滤
   * filter(func:RDD元素类型=>Boolean):按照指定条件过滤
   *    filter是保留函数返回为true的元素
   *    filter里面的func函数是针对RDD每个元素进行操作，函数执行次数=元素个数
   *    用于过滤脏数据
   */
  @Test
  def filter(): Unit = {
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7), 3)
    val rdd2 = rdd.filter(x=>{
      println(s"${Thread.currentThread().getName} -- ${x}")
      x % 2 == 0
    })
    println(rdd2.collect().toList)
  }


}
