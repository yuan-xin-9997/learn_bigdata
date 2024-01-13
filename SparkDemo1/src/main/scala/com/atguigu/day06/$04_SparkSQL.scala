package com.atguigu.day06

import org.apache.spark.sql.Dataset
import org.junit.Test

class $04_SparkSQL {
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  val spark = SparkSession.builder()
      .appName("spark-sql")
      .master("local[4]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
  import spark.implicits._
  /**
   * SparkSQL编程方式
   *    命令式：使用对应方法操作数据
   *    声明式：使用sql操作数据（重点）
   */

  /**
   * 命令式，常用方法
   *    1. 列裁剪
   *    2、过滤
   *    3、去重
   */
  @Test
  def command(): Unit = {
    val list: List[Person] = List(
      Person(1, "zhangsan", "man", 11),
      Person(2, "lisi", "woman", 12),
      Person(3, "wangwu", "man", 13),
      Person(4, "wangwu", "man", 25),
      Person(5, "wangwu", "man", 100),
      Person(5, "wangwu", "man", 100)
    )
    val df = list.toDF()
    df.show()

    // todo 列裁剪:select
    //    SQL 中的select可以很方便的实现别名as和函数操作
    //    Spark SQL 的列裁剪方法select()不支持as和函数操作，一般使用selectExpr()方法，等价与SQL中的select
    val df1 = df.select("name", "age")
    df1.show()
    val df2 = df.selectExpr("name", "age+10 as new_age")
    df2.show()

    // todo 过滤，where 和 filter 等价
    //      where(SQL过滤条件)
    //      filter(SQL过滤条件)
    val df3: Dataset[Row] = df.where("age> 20")
    val df4: Dataset[Row] = df.filter("age> 20")
    df3.show()
    df4.show()

    // todo 去重
    //    distinct:  什么参数都不用传就是 所有字段相同 才区充分
    //    dropDuplicates:  指定列相同去重
    val df5 = df.distinct()   // 什么参数都不传就是 所有字段相同 才区充分
    df5.show()
    val df6: Dataset[Row] = df.dropDuplicates("name")
    df6.show()
  }

  /**
   * 声明式：使用sql操作数据（重点）
   *    1. 将dataFrame/DataSet数据集注册成表
   *    2. sql 操作数据：spark.sql("SQL语句")
   */
  @Test
  def statement(): Unit = {
    val list: List[Person] = List(
      Person(1, "zhangsan", "man", 11),
      Person(2, "lisi", "woman", 12),
      Person(3, "wangwu", "man", 13),
      Person(4, "wangwu", "man", 25),
      Person(5, "wangwu", "man", 100),
      Person(5, "wangwu", "man", 100)
    )
    val df = list.toDF()
    df.show()

    // todo 1. 将dataFrame/DataSet数据集注册成表
    df.createOrReplaceTempView("person")

    // todo 2. sql 操作数据：spark.sql("SQL语句")
    val df1: DataFrame = spark.sql(
      """
        |select
        | name, age
        |from person
        |where age >= 24
        |""".stripMargin
    )
    df1.show()

  }
}
