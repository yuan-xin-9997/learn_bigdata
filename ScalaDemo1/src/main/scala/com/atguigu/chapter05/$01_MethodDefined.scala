package com.atguigu.chapter05

object $01_MethodDefined {
  def main(args: Array[String]): Unit = {
    val a:Int = add(10, 30)
    println(a)
  }

  /*
  * 定义方法
  * def 方法名(参数名:类型, ...):返回值类型={方法体}
  * */
  def add(x:Int, y:Int):Int = {
    return x + y
  }



}
