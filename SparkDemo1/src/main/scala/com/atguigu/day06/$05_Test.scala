package com.atguigu.day06

import org.apache.spark.sql.Dataset

object $05_Test {
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  val spark = SparkSession.builder()
      .appName("spark-sql")
      .master("local[4]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
  import spark.implicits._

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 读取数据
    val ds: Dataset[String] = spark.read.textFile("datas/wc.txt")

    // 创建表
    ds.createOrReplaceTempView("wordcount")

    // todo word count 案例  by Spark SQL
    ds.show()
    val ds1: DataFrame = spark.sql(
      """
        |select
        |wc,
        |count(1)
        |from wordcount
        |Lateral View explode(split(value, " ")) tmp as wc
        |group by wc
        |""".stripMargin
    )
    ds1.show()

  }
}
