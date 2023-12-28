package com.atguigu.chapter05functionalprogramming

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object $03_MethodParameter {
  /**
   * scala中的方法参数，使用比较灵活。它支持以下几种类型的参数：
   *
   * 默认参数
   * 带名参数
   * 变长参数
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    println(add(10, 20))
    println(add2())
//    println(add2) // 不可以省略()
    println(add3(500))
    println(add4(y=500))
    println(add4(y=500, x=500)) // 带名参数
    println(add5(1, 2, 3, 4, 5, 6))  // 可变参数

    val arr = Array(1,2,3,4,5)
//    add5(arr) // 报错，集合不可以传递给可变参数函数

    println(getPaths("/usr/bin", 10))
    val paths = getPaths("/c/d", 10)
    println(paths)
    // 集合不可以传递给可变参数函数，如果要传，则需要使用:_*
    readPaths(paths:_*)
    // add.apply(1,2)
  }

  def add(x:Int, y:Int):Int=x+y
  def add2(x:Int=100, y:Int=200):Int=x+y
  def add3(x:Int, y:Int=100):Int=x+y
  def add4(x:Int=100, y:Int):Int=x+y

  // 变长参数
  def add5(x:Int, y:Int, z:Int*)=x+y+z.sum

  def getPaths(pathPrefix:String, n:Int)={
    // 调用java方法获取当前日期
    val currentDate = LocalDateTime.now();

    // 遍历拼接
    for(i<-1 to n) yield{
      // 日期加减法
      val time=currentDate.plusDays(-i)

      // 格式化日期
      val timestr=time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

      // 插值表达式
      s"${pathPrefix}/${timestr}"
    }
  }

  def readPaths(paths:String*)={
    println(paths)
  }
}
