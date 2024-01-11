package com.atguigu.day06

import org.apache.spark.sql.SparkSession

object $01_SparkSession {
  /**
   * SparkSession是Spark最新的SQL查询起始点，实质上是SQLContext和HiveContext的组合，所以在SQLContext和HiveContext上可用的API在SparkSession上同样是可以使用的。
   * SparkSession内部封装了SparkContext，所以计算实际上是由SparkContext完成的。
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 创建SparkSession的两种方式，常用第2种
    val spark1: SparkSession = new SparkSession.Builder().appName("spark-sql").master("local[4").getOrCreate()
    val spark2: SparkSession = SparkSession.builder().appName("spark-sql").master("local[4").getOrCreate()


  }
}
