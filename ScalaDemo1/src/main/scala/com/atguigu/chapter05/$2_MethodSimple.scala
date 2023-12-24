package com.atguigu.chapter05

object $2_MethodSimple {
  /**
   *
   * def 方法名(参数名:类型, ...):返回值类型={方法体}
   * 方法的简化原则：
   *    1、如果方法体中只有一行语句，方法体{}可以省略
   * @param args
   */
  def main(args: Array[String]): Unit = {
    println(add1(10, 20))
    println(add2(10, 20))
  }

  def add1(x: Int, y: Int): Int = {
    x + y
  }

  // 1、如果方法体中只有一行语句，方法体{}可以省略
  def add2(x:Int, y:Int):Int = x + y
}
