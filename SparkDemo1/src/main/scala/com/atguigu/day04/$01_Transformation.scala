package com.atguigu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class $01_Transformation {
  val conf = new SparkConf().setMaster("local[4]").setAppName("test").set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  /**
   * Key-Value类型 算子
   * aggregateByKey(默认值)(combiner:(默认值类型,Value值类型)=>默认值类型，reducer:(默认值类型,默认值类型)=>默认值类型):
   *    combiner函数是combiner预聚合阶段的聚合逻辑
   *        第一个参数代表该组上一次聚合结果，第一次聚合的时候初始值=默认值
   *        第二个参数代表该组待聚合的value值
   *    reducer函数是reduce预聚合阶段的聚合逻辑
   *        第一个参数代表该组上一次聚合结果，第一次聚合时候初始值=第一个value的值
   *        第二个参数代表该组待聚合的value值
   *
   * 与reduceByKey的区别在于，aggregateByKey中的combiner函数和reducer函数可以不一样，而reduceByKey必须一样
   */
  @Test
  def aggregateByKey(): Unit = {
    // 统计stu_score.txt中每门学科的平均分  使用 aggregateByKey
    val rdd1 = sc.textFile("datas/stu_score.txt")
    println(rdd1.getNumPartitions)

    val rdd2 = rdd1.map(line => {
      val arr = line.split(",")
      val xueke_name = arr(1)
      val score = arr(2).toInt
      (xueke_name, score)
    })
    println(rdd2.collect().toList)

    rdd2.mapPartitionsWithIndex((index, it) => {
      println(s"index=${index} -- data=${it.toList}")
      it
    }).collect()

    val rdd3 = rdd2.aggregateByKey((0, 0))(
      (agg, curr) => {
        println(s"${Thread.currentThread().getName} -- combiner计算：agg=${agg} curr=${curr}")
        (agg._1 + curr, agg._2 + 1)
      },
      (agg, curr) => {
        println(s"reducer计算：agg=${agg} curr=${curr}")
        (agg._1 + curr._1, agg._2 + curr._2)
      }
    )
    println(rdd3.collect().toList)

    val rdd4 = rdd3.map {
      case (name, (score, num)) => (name, score.toDouble / num)
    }
    println(rdd4.collect().toList)
  }


  /**
   * Key-Value类型 算子
   * sortByKey:按照K进行排序
   *
   * 注意与sortBy区别，sortBy是Value类型才能使用的算子，ortBy(func:RDD元素类型=>K): 根据指定字段排序
   *            sortBy里面的函数func，针对每个元素进行操作，元素数量=函数执行次数，根据函数的返回值对RDD元素进行重新排序
   *
   * sortBy比sortByKey更灵活，sortBy可以针对任意值排序，sortByKey只能针对Key排序
   */
  @Test
  def sortByKey(): Unit = {
    val rdd = sc.parallelize(List(1,4,7,3,4,5))

    val rdd2 = rdd.map(x => (x, null))

    val rdd3 = rdd2.sortByKey(ascending = false)
    val rdd4 = rdd2.sortBy(x => x._1)  // 使用sortBy完全可以实现sortByKey的功能
    println(rdd3.collect().toList)
    println(rdd4.collect().toList)
  }

  /**
   *  Key-Value类型 算子
   * mapValues(func:RDD元素类型=>RDD元素类型):一对一映射，原RDD元素的value值计算得到新RDD的value值，元素的key不变
   *    func函数针对每个元素的value值操作，元素个数=func函数执行次数
   *
   * 完全可以由map来替代
   */
  @Test
  def mapValues(): Unit = {
    val rdd = sc.parallelize(List("a"->10, "b"->20, "c"->30, "d"->40))

    val rdd2 = rdd.mapValues(x => x / 10)
    val rdd3 = rdd.map(x => x._2 / 10)

    println(rdd2.collect().toList)
    println(rdd3.collect().toList)
  }

  /**
   * join；相当于SQL的inner join
   *    join生成的新RDD元素类型是(K, (左RDD value值，右RDDvalue值))
   *    两个RDD key的元素类型相同才可以Join操作
   */
  @Test
  def join(): Unit = {
    val rdd1 = sc.parallelize(List("a"->10, "b"->20, "c"->30, "d"->40,"a"->100, "e"->200))
    val rdd2 = sc.parallelize(List("a"->10.1, "b"->20.1, "c"->30.1, "d"->40.1,"f"->300))

    // inner join = 左右表能连接的数据
    val rdd3 = rdd1.join(rdd2)
    println(rdd3.collect().toList)

    // left join = 左右表能连接的数据 + 左表不能连接的数据
    val rdd4 = rdd1.leftOuterJoin(rdd2)
    println(rdd4.collect().toList)

    // right join = 左右表能连接的数据 + 右表不能连接的数据
    val rdd5 = rdd1.rightOuterJoin(rdd2)
    println(rdd5.collect().toList)

    // full join = 左右表能连接的数据 + 右表不能连接的数据 + 左表不能连接的数据
    val rdd6 = rdd1.fullOuterJoin(rdd2)
    println(rdd6.collect().toList)
  }

  /**
   * cogroup = groupByKey + fullOuterJoin
   */
  @Test
  def cogroup(): Unit = {
    val rdd1 = sc.parallelize(List("a" -> 10, "b" -> 20, "c" -> 30, "d" -> 40, "a" -> 100, "e" -> 200))
    val rdd2 = sc.parallelize(List("a" -> 10.1, "b" -> 20.1, "c" -> 30.1, "d" -> 40.1, "f" -> 300.1))

    val rdd3 = rdd1.cogroup(rdd2)
    println(rdd3.collect().toList)

    // -----------------使用groupByKey + fullOuterJoin实现cogroup----------------
    // println(rdd1.groupByKey().collect().toList)
    val rdd11: RDD[(String, Iterable[Int])] = rdd1.groupByKey()
    val rdd22: RDD[(String, Iterable[Double])] = rdd2.groupByKey()

    val rdd33: RDD[(String, (Option[Iterable[Int]], Option[Iterable[Double]]))] = rdd11.fullOuterJoin(rdd22)
    println(rdd33.collect().toList)

  }


}
