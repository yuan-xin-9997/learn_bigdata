package com.atguigu.chapter09exception

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.util.Try

object $01_Exception {


  /**
   * java的异常处理方式:
     * 1、捕获异常: try{..}catch(xxxException e){...}...finally{...}
     * 2、抛出异常
     * 1、在方法体中通过throw关键字抛出异常
     * 2、方法名后面通过throws关键字声明异常
   *
   * scala的异常处理方式:
   * 1、捕获异常:  <工作常用>
     * 1、try{..}catch{...}finally{...}  <一般用于获取外部资源链接场景>
     * 2、Try(代码块).getOrElse(默认值) <常用>
       * 如果Try中的代码块执行成功返回的是Success对象,此时getOrElse返回代码块执行结果
       * 如果Try中的代码块执行异常返回的是Failure对象,此时getOrElse返回默认值
   *
   * 2、抛出异常
   *    1、在方法体中通过throw关键字抛出异常
   *
   */
  def main(args: Array[String]): Unit = {
    // 捕获异常
    println(m1(10, 2))
    println(m1(10, 0))


    // 抛出异常
    println(m2(10, 2))
    // println(m2(10, 0))


    val list = List( "1\tlisi\t20\tshenzhen","2\twagnwu\t\tbeijing","3\t\t40\tshanghai","4\t\t\tbeijing" )
    val list2 = list.filter(line => {
      val arr = line.split("\t")
      arr(1) != "" ||  arr(2) != ""
    })
    println(list2)

    val list3 = list2.map(x => {
      val arr = x.split("\t")
      val region = arr.last
      // val age = try{arr(2).toInt} catch {case e:Exception=>0 }
      val age = Try.apply( arr(2).toInt  ).getOrElse(0)   // 功能与上面代码一致
      (region, age)
    })
    println(list3)

  }

  /**
   * 捕获异常
   *
   * @param a
   * @param b
   * @return
   */
  def m1(a: Int, b: Int) = {
    try {
      a / b
    } catch {
      case e: ArithmeticException =>
        println("被除数不能为0")
        -1
      case e: Exception =>
    }
  }

  /**
   * 抛出异常
   *
   * @param a
   * @param b
   * @return
   */
  def m2(a: Int, b: Int) = {
    if (b == 0)
      throw new Exception("被除数不能为0...")
    a / b
  }


  def jdbUntil(): Unit = {

    var connection: Connection = null
    var statement: PreparedStatement = null
    try {

      //创建连接
      connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root123")
      //创建statement对象
      statement = connection.prepareStatement("insert into person values(?,?)")

      //sql参数赋值
      statement.setString(1, "lisi")
      statement.setInt(2, 10)

      //执行sql
      statement.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      //关闭连接
      if (statement != null)
        statement.close()
      if (connection != null)
        connection.close()
    }
  }

}
