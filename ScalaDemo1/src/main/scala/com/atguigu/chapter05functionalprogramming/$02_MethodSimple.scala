package com.atguigu.chapter05functionalprogramming

object $02_MethodSimple {
  /**
   *
   * def 方法名(参数名:类型, ...):返回值类型={方法体}
   * 方法的简化原则：
   *    1、如果方法体中只有一行语句，方法体{}可以省略
   *    2、返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
   *       注意：如果有return，则不能省略返回值类型，必须指定
   *    3、如果方法不需要参数，参数列表的()可以省略
   *        注意：
   *          1、如果定义方法的时候省略了参数列表()。在调用方法的时候不能带上()
   *          2、如果定义方法的时候没有省略参数列表()。调用方法的时候()可有可无
   *    4、如果方法不需要返回值，=可以省略
   * @param args
   */
  def main(args: Array[String]): Unit = {
    println(add(10, 20))
    println(add2(10, 20))
    println(add3(10, 20))
    println(add4(10, 0))
    printHello()
  }

  // 标准写法
  def add(x: Int, y: Int): Int = {
    x + y
  }

  // 1、如果方法体中只有一行语句，方法体{}可以省略
  def add2(x:Int, y:Int):Int = x + y

  // 2. 返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
  def add3(x:Int, y:Int) = x + y

  // 如果有return，则不能省略返回值类型，必须指定
  def add4(x:Int, y:Int): Int ={
    if(y==0)
      return -1
    x + y
  }

  //如果方法不需要参数，参数列表的()可以省略
  def printHello()=println("hello")

  // 4、如果方法不需要返回值，=可以省略   ()和{}不可以同时省略
  def printHello1() {
    println("hello")
  }
}
