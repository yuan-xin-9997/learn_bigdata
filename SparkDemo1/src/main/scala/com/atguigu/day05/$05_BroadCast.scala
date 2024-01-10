package com.atguigu.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class $05_BroadCast {
  val conf = new SparkConf().setMaster("local[4]").setAppName("test").set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  /**
   *
   * 广播变量使用场景：
   *      1、在spark算子中使用了Driver的数据，并且该数据量较大。
   *            好处：可以节省内存空间占用，原来占用内存空间 task个数*数据大小，使用之后占用空间大小 Executor个数*数据大小
   *      2、大表 join 小表：将小表广播，类似于Hadoop中将小表缓存到Map端，不用走shuffle即可Join
   *            好处：减少shuffle操作
   *
   * 使用：
   *      1、广播Driver数据：val bc =  sc.broadcast(数据)
   *      2、Task从Executor获取数据：bc.value
   */

  /**
   * 使用广播之前
   */
  @Test
  def Normal: Unit = {
    val rdd = sc.parallelize(List("atguigu", "jd", "pdd", "tb"))
    val map = Map("atguigu"->"http://www.atguigu.com/")
    val rdd2 = rdd.map(x => map.getOrElse(x, null))
    println(rdd2.collect().toList)
    Thread.sleep(10000000)
  }

  /**
   * 使用场景：1、在spark算子中使用了Driver的数据，并且该数据量较大。
   */
  @Test
  def Test1(): Unit = {

    val rdd = sc.parallelize(List("atguigu", "jd", "pdd", "tb"))
    val map = Map("atguigu" -> "http://www.atguigu.com/")

    // todo 广播数据
    val bc = sc.broadcast(map)

    val rdd2 = rdd.map(x => {
      // todo 取出广播变量，并使用
      val mp = bc.value
      mp.getOrElse(x, null)
    })
    println(rdd2.collect().toList)
    Thread.sleep(10000000)
  }

  /**
   * 使用场景2：大表join小表，直接使用leftOutJoin
   */
  @Test
  def Test21(): Unit = {

    val stuRDD = sc.textFile("datas/student.txt")
    val courseRDD = sc.textFile("datas/course.txt")

    // todo 需求：获取学生信息和所学科目名称
    val mapstuRDD = stuRDD.map(line => {
      val arr = line.split(",")
      val id = arr.head
      val name = arr(1)
      val timeStr = arr(2)
      val sex = arr(3)
      val courseID = arr.last
      (courseID, (id, name, timeStr, sex))
    })

    val mapcourseRDD = courseRDD.map(line => {
      val arr = line.split(",")
      val id = arr.head
      val name = arr.last
      (id, name)
    })

    // 左外join
    val resRDD: RDD[(String, ((String, String, String, String), Option[String]))] = mapstuRDD.leftOuterJoin(mapcourseRDD)

    // 使用偏函数，模式匹配，处理结果
    val res = resRDD.map {
      case (courseID, ((id, name, timeStr, sex), courseName)) => (id, name, timeStr, sex, courseName.getOrElse(null))
    }

    // todo: 实现该join需求了，但是左外连接有shuffle操作，且2次，共3个stage

    // 打印结果
    res.foreach(println)

    Thread.sleep(100000)
  }

  /**
   * 使用场景2：大表join小表，todo 使用广播变量，广播小表
   */
  @Test
  def Test22(): Unit = {

    val stuRDD = sc.textFile("datas/student.txt")
    val courseRDD = sc.textFile("datas/course.txt")

    // todo 需求：获取学生信息和所学科目名称
    val mapstuRDD = stuRDD.map(line => {
      val arr = line.split(",")
      val id = arr.head
      val name = arr(1)
      val timeStr = arr(2)
      val sex = arr(3)
      val courseID = arr.last
      (courseID, (id, name, timeStr, sex))
    })

    val mapcourseRDD = courseRDD.map(line => {
      val arr = line.split(",")
      val id = arr.head
      val name = arr.last
      (id, name)
    })


    // todo 广播小表数据
    //    收集数据到Driver端
    //    RDD 没有数据，不能广播RDD
    val cMap: Map[String, String] = mapcourseRDD.collect().toMap
    val bc = sc.broadcast(cMap)

    // 使用广播变量 实现大表join小表
    val res = mapstuRDD.map {
      case (courseID, (id, name, timeStr, sex)) => {
        // todo 使用广播数据
        val tmopCMap = bc.value
        val courseName = tmopCMap.getOrElse(courseID, null)
        (id, name, timeStr, sex,  courseName)
      }
    }

    // 打印结果
    res.foreach(println)

    Thread.sleep(100000)
  }

}
