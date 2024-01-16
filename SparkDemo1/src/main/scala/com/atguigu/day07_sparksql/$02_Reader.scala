package com.atguigu.day07_sparksql

import org.apache.spark.sql.SaveMode
import org.junit.Test

import java.util.Properties

/**
 * SparkSQL数据的加载
 */
class $02_Reader {
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  val spark = SparkSession.builder()
      .appName("spark-sql")
      .master("local[4]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
  import spark.implicits._
  /**
   * SparkSQL读取数据有两种方式：
   *   1. spark.read  【工作不用】
   *        .format("csv/text/parquet/jdbc/orc")  -- 指定读取数据的格式
   *        [.option(k,v)...]  -- 读取数据需要的参数
   *        .load([path]) -- 加载数据
   *   2. spark.read.[option(k,v)...].json/csv/parquet/orc/jdbc(...)  【工作常用】
   */

  /**
   * 读取文本文件
   */
  @Test
  def readFile(): Unit = {
    // todo 读取文件第一种方式
    val df: DataFrame = spark.read.format("text").load("datas/wc.txt")
    df.show()

    // todo 读取文件第二种方式
    val df1: DataFrame = spark.read.text("datas/wc.txt")
    df1.show()

    // 读取json文件
    val df3: DataFrame = spark.read.json("datas/test.json")
    df3.show()

    // 读取csv文件
    //   常用option：
    //      sep 指定字段之间的分隔符，默认为 ,
    //      header (default false): uses the first line as names of columns
    //      inferSchema (default false): 自动推断列的类型,infers the input schema automatically from data. It requires one extra pass over the data
    //
    val df4: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("datas/双十一淘宝美妆数据_head.csv")
    df4.show()
    df4.printSchema()

    // 读取parquet文件（列式存储文件）
    df4.write.mode(SaveMode.Overwrite).parquet("output/parquet")
    spark.read.parquet("output/parquet").show()

    // 读取orc文件
    df4.write.mode(SaveMode.Overwrite).orc("output/orc")
    spark.read.orc("output/orc").show()
  }

  /**
   * 读取MySQL文件
   */
  @Test
  def readMySQL(): Unit = {
    // 指定mysql url地址
    val url = "jdbc:mysql://hadoop102:3306/gmall"

    // 指定读取的表名
    // 下面读取整表数据
    val tableName = "user_info"
    // val tableName = "(select * from user_info where id>50) t1"

    // 指定读取Mysql需要的参数封装
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")

    // todo 读取mysql数据第一种方式，
    //   此处方式生成的DataFrame 只有1个分区，如果表数据量很大，此处可能会卡死。  【工作不用】
    //   只适用于小数据的量的方式
    val df1: DataFrame = spark.read.jdbc(url, tableName, properties)//.filter("id>50")
    df1.show()
    println(df1.rdd.getNumPartitions)   // todo  1, 只有1个分区，如果表数据量很大，此处可能会卡死


    // todo 读取mysql数据第二种方式，
    //  生成的DataFrame分区数=conditions数组种的元素个数  【工作不用】
    //  每个分区拉去数据数据的条件，predicates – Condition in the where clause for each partition.
    val conditions = Array("id<=10", "id > 10 and id <= 20", "id > 20 and id < 50", "id >= 50")
    val df2: DataFrame = spark.read.jdbc(url, tableName, conditions, properties)
    df2.show()
    println(df2.rdd.getNumPartitions)  // 4   几个条件对应几个分区
    df2.write.mode(SaveMode.Overwrite).json("output/mysql")



    // todo 读取mysql数据第三种方式，大数据量场景常用  【工作常用】
    //   此种方式读取mysql生成的DataFrame的分区数 = (upperBound - lowerBound) < numPartitions ? (upperBound - lowerBound)  :   numPartitions
    //   columnName – the name of a column of numeric, date, or timestamp type that will be used for partitioning.
    //   lowerBound – the minimum value of columnName used to decide partition stride.
    //   upperBound – the maximum value of columnName used to decide partition stride.
    //   numPartitions – the number of partitions. This, along with lowerBound (inclusive), upperBound (exclusive), form partition strides for generated WHERE clause expressions used to split the column columnName evenly. When the input is less than 1, the number is set to 1.

    // lower bound 和 upper bound 应该动态查询
    val tName = "(select min(id), max(id) from user_info) user"
    val minMaxIdDF: DataFrame = spark.read.jdbc(url, tName, properties)
    minMaxIdDF.show()
    val row: Row = minMaxIdDF.first()
    val low = row.getAs[Long]("min(id)")
    val upper  = row.getAs[Long]("max(id)")

    val columnName = "id"  // 用于分区的mysql字段名，该字段类型必须是数字、日期、时间戳类型，建议用主键字段
    val lowerBound = low // 用于决定分区数据间距的下限，一般设置为columnName字段的最小值
    val upperBound = upper // 用于决定分区数据间距的上限，一般设置为columnName字段的最大值
    val df3 = spark.read.jdbc(url, tableName, columnName, lowerBound, upperBound, 5, properties)
    df3.show()
    println(df3.rdd.getNumPartitions)  // 此种方式读取mysql生成的DataFrame的分区数 = (upperBound - lowerBound) < numPartitions ? (upperBound - lowerBound)  :   numPartitions
    df3.write.mode(SaveMode.Overwrite).csv("output/mysql/csv")

  }

}
