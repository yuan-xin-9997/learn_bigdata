package com.atguigu.day04

import org.apache.spark.{SparkConf, SparkContext}

object $07_Dependency {
  /**
   * 依赖：父子RDD之间的RDD的关系
   * 依赖关系分为两种：
   *      宽依赖：有shuffle的为宽依赖  org.apache.spark.ShuffleDependency
   *      窄依赖：没有shuffle的为窄依赖 org.apache.spark.NarrowDependency，分为：
   *            org.apache.spark.OneToOneDependency 一个分区对一个分区
   *            org.apache.spark.RangeDependency union算子生成的RDD为此类依赖
   * todo Application：提交的YARN的任务叫应用，一个SparkContext称之为一个应用
   *          Job：要运行的任务，一般一个action算子产生一个job
   *              stage：阶段，一个job中的stage个数 = shuffle个数+1
   *                    task：子任务，一个stage中task的个数，=stage中最后一个rdd最后一个rdd的分区数
   * todo 并行关系说明：
   *        一个Application中多个job之间是串行执行
   *        一个job中多个stage之间是串行（参考Map阶段结束后，才能进行reduce阶段）
   *        一个stage中多个task之间是并行
   *
   * stage切分
   *        Action算子，例如collection，其实sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
   *        执行job的时候，会根据依赖关系，从最后一个RDD，依次往前寻找，直到第一个RDD，当遇到宽依赖，则切分stage；
   *        执行job是从开始往后执行
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)

    println("--------------------------")
    val rdd = sc.textFile("datas/wc.txt") // textFile生成两个RDD
    println(rdd.dependencies)
    println("--------------------------")
    val rdd2 = rdd.flatMap(line => line.split(" "))
    println(rdd2.dependencies)
    println("--------------------------")
    val rdd3 = rdd2.map(x => (x, 1))
    println(rdd3.dependencies)
    println("--------------------------")
    val rdd4 = rdd3.reduceByKey(_ + _)
    println(rdd4.collect().toList)
    println(rdd4.collect().toList)
    println(rdd4.dependencies)
    println("--------------------------")
    Thread.sleep(10000000)

  }
}
