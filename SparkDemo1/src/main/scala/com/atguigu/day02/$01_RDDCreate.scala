package com.atguigu.day01
import org.apache.spark.{SparkConf, SparkContext}


object $01_RDDCreate {
  val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)
  /**
   * RDD的创建方式：
   *    1. 通过本地集合创建
   *    2. 通过读取文件创建
   *    3. 通过其他的RDD衍生
   * @param args
   */
  def main(args: Array[String]): Unit = {
    createRDDByCollection()
    createRDDByFile()
    createRDDByRDD()
  }

  /**
   * 1. 通过本地集合创建，一般用于测试
   */

  def createRDDByCollection(): Unit = {
    println(".......")

    val list = List(1,4,3,6)
    val rdd = sc.makeRDD(list)
    println(rdd.collect())
    println(rdd.collect().toList)

    val rdd2 = sc.parallelize(list)
    println(rdd2.collect().toList)
  }

  /**
   * 2. 通过读取文件创建
   */
  def createRDDByFile(): Unit = {
    println("------------------")
    val rdd = sc.textFile("datas/wc.txt")
    println(rdd.collect())
    println(rdd.collect().toList)

//    System.setProperty("user.name", "atguigu")

//    System.setProperty("HADOOP_USER_NAME", "atguigu");
    val rdd2 = sc.textFile("hdfs://hadoop102:8020/user/hive/warehouse/user_session")
    println(rdd2.collect())
    println(rdd2.collect().toList)
  }

  /**
   * 3. 通过其他的RDD衍生
   */
  def createRDDByRDD(): Unit = {
    println("-------------------")
    val rdd = sc.textFile("datas/wc.txt")

    val rdd2 = rdd.flatMap(x => x.split(" "))
    println(rdd2.collect().toList)
  }


}
