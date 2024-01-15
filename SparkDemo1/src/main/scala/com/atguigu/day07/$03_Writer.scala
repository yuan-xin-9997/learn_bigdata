package com.atguigu.day07

import org.apache.spark.sql.SaveMode
import org.junit.Test

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

class $03_Writer {
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  val spark = SparkSession.builder()
      .appName("spark-sql")
      .master("local[4]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
  import spark.implicits._
  /**
   * SparkSQL保存数据有两种方式：
   * 1. ds/df.write  【工作不用】
   *          .mode(SaveMode.XXX)  -- 指定写入模式
   *          .format("csv/json/parquet/jdbc/text")  -- 写入文件的个数
   *          .[option(k,v)...].
   *          .save([path])  -- 保存路径
   * 2. spark.write.mode(SaveMode.XXX).[option(k,v)...].json/csv/parquet/orc/jdbc(...)  【工作常用】
   *
   *
   * 常用写入模式：
   *      SaveMode.Overwrite: 如果保存数据的目录/表存在，则覆盖数据【先删目录/表，再写数据】【一般用于写入HDFS】
   *      SaveMode.Append: 如果保存数据的目录/表存在，则追加数据【一般用于将数据写入 没有主键 的mysql表中】
   */

  /**
   * 写入数据到文本文件
   */
  @Test
  def writeFile(): Unit = {

    val df = spark.read.json("datas/test.json")

    // 写json文件
    // todo 写入文件的第一种方式
    df.write.mode(SaveMode.Overwrite).format("json").save("output/json")
    // todo 写入文件的第二种方式
    df.write.mode(SaveMode.Overwrite).json("output/json")

    // 写csv文件，可以添加option，类似读取文件
    df.write.mode(SaveMode.Overwrite).option("sep", "#").option("header", "true").csv("output/csv")

    // 写入parquet文件
    df.write.mode(SaveMode.Overwrite).parquet("output/parquet")

    // 写入文本文件
    //   报错：org.apache.spark.sql.AnalysisException: Text data source does not support bigint data type.; 写入到文本文件时只允许有一个列
    //df.write.mode(SaveMode.Overwrite).text("output/text")
    df.toJSON.write.mode(SaveMode.Overwrite).text("output/text")
  }

  @Test
  def writeMySQL(): Unit = {
    val df: DataFrame = spark.read.json("datas/test.json")

    val url = "jdbc:mysql://hadoop102:3306/test"
    val tableName = "test"

    // 指定读取Mysql需要的参数封装
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")

    // todo 写入数据到MySQL，如果MySQL表有主键，且主键重复，则下面的代码会报错
    //df.write.mode(SaveMode.Overwrite).jdbc(url, tableName, properties)
    //df.write.mode(SaveMode.Append).jdbc(url, tableName, properties)

    // todo 往有主键的表写入数据，并且在冲突的时候，更新数据
    df.rdd.foreachPartition(it=>{
      var connection:Connection=null
      var statement:PreparedStatement=null
      try{
        connection=DriverManager.getConnection(url, "root", "root")
        statement=connection.prepareStatement("insert into test(id, Name, age, sex) values (?,?,?,?) on duplicate key update name=?, age=?,sex=?")
        var i = 0
        it.foreach(row=>{
          val id = row.getAs[Long]("id")
          val name = row.getAs[String]("name")
          val age = row.getAs[Long]("age")
          val sex = row.getAs[String]("sex")
          statement.setLong(1, id)
          statement.setString(2, name)
          statement.setLong(3, age)
          statement.setString(4, sex)
          statement.setString(5, name)
          statement.setLong(6, age)
          statement.setString(7, sex)
          statement.addBatch()
          if(i%2==0){  // 1000条SQL语句为一个批次
            println("执行批次")
            statement.executeBatch()
            statement.clearBatch()
          }
          i = i + 1
          statement.executeBatch()  // 执行最后不到1000条SQL语句批次
        })
      }catch {
        case e:Exception=>e.printStackTrace()
      }finally {
        if(statement!=null)
          statement.close()
        if(connection!=null)
          connection.close()
      }
    })

    spark.stop()

  }

}
