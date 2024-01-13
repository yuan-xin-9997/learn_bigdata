package com.atguigu.day06

import org.apache.spark.sql.Dataset

object $08_Demo {

  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  val spark = SparkSession.builder()
      .appName("spark-sql")
      .master("local[4]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
  import spark.implicits._
  /**
   * 案例：双十一淘宝美妆数据
   *
   * 如果title中包含某一个类别的关键字，那么取出主类与子类补充到元数据
   * 如果title中出现了男士、男生、男关键字，那么为男士专用
   * 1、查看每个品牌的商品数量，降序排序
   * 2、查看每个品牌的销售量，降序排序
   * 2、查看每个品牌的销售额，降序排序
   * 3、求得每个主类的销量占比情况
   * 3、求得每个子类的销量占比情况
   * 4、各品牌各主类的总销售额
   * 4、各品牌各子类的总销售量
   * 5、各品牌商品的平均评论数
   * 6、各品牌商品的平均价格
   * 7、各品牌男性专用商品的销量情况
   * 7、各品牌男性专用商品的销售额情况
   * 8、每月的销量情况
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 读取数据
    val datasDF = spark.read.csv("datas/双十一淘宝美妆数据.csv").toDF("update_time", "id", "title", "price", "sale_count", "comment_count", "name")

    val rawTypeDS: Dataset[String] = spark.read.textFile("datas/type.txt")
    datasDF.show()
    rawTypeDS.show()

    rawTypeDS.flatMap(line=>{

    })
  }
}
