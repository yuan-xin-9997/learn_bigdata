package com.atguigu.day06_sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test


class $02_DataFrame {

  val spark = SparkSession.builder()
    .appName("spark-sql")
    .master("local[4]")
    .config("spark.testing.memory", "2147480000")
    .getOrCreate()

  /**
   * DataFrame的创建方式：
   * 1、通过toDF的方式创建
   * 2、通过读取文件创建
   * 3、通过其他的DataFrame衍生
   * 4、通过createDataFrame方法创建

   */


  /**
   * 1. 通过toDF方法创建
   *    集合.toDF
   *    RDD.toDF
   *        集合/RDD中元素类型是样例类的时候，转成DataFrame之后，列名默认就是属性名
   *        集合/RDD中元素类型是元组的时候，转成DataFrame之后，列名默认就是_N
    */
  @Test
  def createDataFrameByToDF(): Unit = {

    val list: List[Person] = List( Person(1, "zhangsan", "man", 11),  Person(2, "lisi", "woman", 12), Person(3, "wangwu", "man", 13) )

    // ------------------------Scala集合.toDF----------------------------
    // todo list是scala的list，没有toDF方法， 需要导入隐式转换，位于SparkSession内部的implicits
    import spark.implicits._
    val df: DataFrame = list.toDF

    // 展示数据，结果是二维表，
    df.show()

    // 展示schema，列信息
    df.printSchema()


    val list2: List[(Int, String, String, Int)] = List( (1, "zhangsan", "man", 11),  (2, "lisi", "woman", 12), (3, "wangwu", "man", 13) )
    val df1 = list2.toDF()  // 不传列名
    df1.show
    df1.printSchema

    val df2 = list2.toDF("ID", "NAME", "SEX", "AGE") // todo 传列名，传入的列名参数必须完全等于元组的维度
    df2.show
    df2.printSchema

    // ------------------------RDD.toDF----------------------------
    val rdd = spark.sparkContext.parallelize(list)
    println(rdd.getNumPartitions)
    println(rdd.collect().toList)

    val df3 = rdd.toDF()
    df3.show
    df3.printSchema
  }

  /**
   * 2. 通过读取文件创建
   *    todo 注意：json文件中一行为一个{}包括的json对象
   */
  @Test
  def createDataFrameByFile(): Unit = {
    val df = spark.read.json("datas/test.json")
    df.show
  }

  /**
   * 3、通过其他的DataFrame衍生
   *    类似于RDD衍生
   */
  @Test
  def createDataFrameByDataFrame(): Unit = {
    val list: List[Person] = List( Person(1, "zhangsan", "man", 11),  Person(2, "lisi", "woman", 12), Person(3, "wangwu", "man", 13) )
    import spark.implicits._
    val df: DataFrame = list.toDF

    val df2 = df.filter("age>=12")
    df2.show()

  }

  /**
   * 4、通过createDataFrame方法创建
   */
  @Test
  def createDataFrameByMethod(): Unit = {
    // 创建List[Row]
    val list: List[Row] = List( Row(1, "zhangsan", "man", 11),  Row(2, "lisi", "woman", 12), Row(3, "wangwu", "man", 13) )

    // 创建RDD[Row]
    val rdd: RDD[Row] = spark.sparkContext.parallelize(list)

    // todo 列信息系封装
    val fields = Array( StructField("id", IntegerType),StructField("name", StringType),StructField("sex", StringType),StructField("age", IntegerType) )
    val schema = StructType(fields)

    // 通过方法创建DataFrame
    val df = spark.createDataFrame(rdd, schema)

    df.show()
    df.printSchema()
  }
}


case class Person(id:Int, name:String, sex:String, age:Int)
