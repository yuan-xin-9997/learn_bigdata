package com.atguigu.day06

object $07_UserDefinedFunctionUDF {
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  val spark = SparkSession.builder()
      .appName("spark-sql")
      .master("local[4]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
  import spark.implicits._

  /**
   * todo 自定义UDF函数：
   *    1. 写一个函数
   *    2、将函数注册到SparkSession中
   *
   * 根据用户自定义函数类别分为以下三种：
   *    ① UDF（User-Defined-Function）--> 一进一出
   *    ② UDAF（User-Defined Aggregation Function） --> 聚合函数，多进一出，类似：count/max/min
   *    ③ UDTF（User-Defined Table-Generating Functions）--> 炸裂函数，一进多出，如：explode()   【SparkSQL没有】
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val list = List(
      ("1001", "lisi1", 21),
      ("1001001", "lisi2", 22),
      ("100111", "lisi3", 25),
      ("210011", "lisi4", 267   ),
      ("21001", "lisi5", 231),
      ("1003231", "lisi6", 10),
    )
    val df: DataFrame = list.toDF("id", "name", "age")

    // 需求: 员工ID应该为8为，不满8位的员工id，左侧用0补齐
    // ref:https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-StringFunctions
    // lpad(string str, int len, string pad)
    // Returns str, left-padded with pad to a length of len. If str is longer than len, the return value is shortened to len characters. In case of empty pad string, the return value is null.
    df.selectExpr( "lpad(id, 8, 0) id", "name", "age" ).show( )

    val str = myLpad("hhhhhhhhh",  8, "0")
    println(str)

    // todo 2、将函数注册到SparkSession中
    spark.udf.register("myLpad", myLpad _)   // todo 方法 _  表示方法转函数
    spark.udf.register("myLpadfunc", myLpadfunc)   // todo myLpadfunc已经是函数，不需转换
    df.selectExpr( "myLpad(id, 8, 0) id", "name", "age" ).show( )
    df.selectExpr( "myLpadfunc(id, 8, 0) id", "name", "age" ).show( )
  }

  /**
   * todo 1. 写一个函数
   * lpad(string str, int len, string pad)
   * Returns str, left-padded with pad to a length of len. If str is longer than len, the return value is shortened to len characters. In case of empty pad string, the return value is null.
   *
   * @param str 待补齐的字符串
   * @param len 最终补齐的长度
   * @param pad 使用pad填充
   */
  def myLpad(str:String, len:Int, pad:String): String = {
    // 字符串长度
    val strlen = str.length
    // 需要补齐的长度
    val padLen = len - strlen
    // 补齐字符串
    s"${pad*padLen}${str}"
  }

  val myLpadfunc: (String, Int, String) => String = (str:String, len:Int, pad:String) => {
    // 字符串长度
    val strlen = str.length
    // 需要补齐的长度
    val padLen = len - strlen
    // 补齐字符串
    s"${pad * padLen}${str}"
  }
}
