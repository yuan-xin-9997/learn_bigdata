package com.atguigu.day04

object $04_ClosePackage {
  def main(args: Array[String]): Unit = {

    // 闭包函数：函数体重使用外部变量的函数，成为闭包
    val x = 10
    val func = (a:Int)=>x+a
    println(func(10))

  }
}
