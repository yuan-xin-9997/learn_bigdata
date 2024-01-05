package com.atguigu.day03

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

class $01_Transformation{

  val conf = new SparkConf()
    .setMaster("local[*]").setAppName("test")
    .set("spark.testing.memory", "2147480000")
    .set("spark.default.parallelism", "10")
  val sc = new SparkContext(conf)

  @Test
  def mapPartitions(): Unit = {
    // RDD 中的元素是用户的id
//    val rdd = sc.parallelize(List(1,4,2,3,7,10))
    val rdd = sc.parallelize(List(1,10,3))

    // 需求：根据id从mysql查询数据获取用户详细的信息
    val rdd2 = rdd.map(id => {
      // 创建连接
      var connection: Connection = null
      var statement: PreparedStatement = null
      var name: String = null
      var age: Int = -1
      try {
        // 创建连接
        connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root")
        // 创建statement对象
        statement = connection.prepareStatement("select * from person where id=?")
        // 赋值
        statement.setInt(1, id)
        // 执行SQL
        val res: ResultSet = statement.executeQuery()
        while (res.next()) {
          name = res.getString("name")
          age = res.getInt("age")
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (statement != null)
          statement.close()
        if (connection != null)
          connection.close()
      }
      (id, name, age)
    })

    println(rdd2.collect().toList)


  }
}