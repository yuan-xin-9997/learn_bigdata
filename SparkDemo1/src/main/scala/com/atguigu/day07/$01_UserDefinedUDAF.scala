package com.atguigu.day07

object $01_UserDefinedUDAF {

  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  val spark = SparkSession.builder()
      .appName("spark-sql")
      .master("local[4]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
  import spark.implicits._

  /**
   * todo 自定义UADF函数：
   *    1. 写一个函数
   *    2、将函数注册到SparkSession中
   *
   * 根据用户自定义函数类别分为以下三种：
   * ① UDF（User-Defined-Function）--> 一进一出
   * ② UDAF（User-Defined Aggregation Function） --> 聚合函数，多进一出，类似：count/max/min
   * ③ UDTF（User-Defined Table-Generating Functions）--> 炸裂函数，一进多出，如：explode()   【SparkSQL没有】
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val list = List(
      ("zhangsan1", 20, "BJ"),
      ("zhangsan2", 30, "SH"),
      ("zhangsan3", 40, "SZ"),
      ("zhangsan4", 50, "GZ"),
      ("zhangsan5", 60, "HZ"),
      ("zhangsan6", 70, "BJ"),
      ("zhangsan7", 20, "BJ"),
      ("zhangsan8", 20, "BJ"),
      ("zhangsan9", 20, "BJ"),
      ("zhangsan10", 20, "BJ"),
    )
    val rdd = spark.sparkContext.parallelize(list, 2)

    println(rdd.getNumPartitions)
    rdd.mapPartitionsWithIndex((index, it)=>{
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()

    val df = rdd.toDF("name", "age", "region")

    // df.selectExpr("region", "avg(age)") // 报错，使用聚合avg函数，region字段必须出现在group by后面
    // df.selectExpr( "avg(age)")

    df.createOrReplaceTempView("person")
    spark.sql(
      """
        |select region,avg(age) from person group by region
        |""".stripMargin).show()


    // 自定义UDAF函数
    //    弱类型定义方式  3.x过期，一般在2.x版本使用
    //    强类型定义方式，3.x版本推荐，2.x版本没有

    // todo 将自定义UDAF函数注册到spark中
    spark.udf.register("myavgl", new WeakAvgUDAF)
    // 使用自定义函数UDAF计算平均值
    spark.sql(
      """
        |select region,myavgl(age) from person group by region
        |""".stripMargin
    ).show()


  }

}
