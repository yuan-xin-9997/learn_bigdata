package com.atguigu.day05

import org.apache.spark.{SparkConf, SparkContext}

object $03_Accumulator {
  val conf = new SparkConf().setMaster("local[4]").setAppName("test").set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  /**
   * 累加器 ：
   *      使用场景：一般用于聚合，并且最终聚合结果不能太大
   *      好处：可以在一定程度上减少shuffle操作
   *      分类：longAccumulator
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    var sum = 0
    val rdd = sc.parallelize(List(10,20,30,40,50),2)
    println(rdd.getNumPartitions)
//    rdd.mapPartitionsWithIndex((index, it) => {
//      println(s"index=${index} data=${it.toList}")
//      it
//    }).collect()

//    rdd.foreach(x=>{
//      sum+=x
//      println(s"sum=${sum}")
//    })
//    println(sum)  // 0

    // todo Spark累加器
    val acc = sc.longAccumulator("sumAcc")
    rdd.foreach(x=>acc.add(x))
    println(acc.value)  // 150

    Thread.sleep(100000)

  }
}
