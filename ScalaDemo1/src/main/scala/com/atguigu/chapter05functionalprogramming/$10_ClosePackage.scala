package com.atguigu.chapter05functionalprogramming

object $10_ClosePackage{

  /**
   * 闭包：就是一个函数和与其相关的引用环境（变量）组合的一个整体(实体)
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val y = 10

    // 闭包函数，函数中使用了外部变量y
    val func=(x:Int)=>x+y

  }
}
