package com.atguigu.chapter08match

import scala.util.Random

/**
 * 类似Java中的switch语句
 * scala模式匹配的语法：
 * 变量 match {
       * case 条件1 => {.....}
       * case 条件2 => {.....}
       * case 条件3 => {.....}
       * ......
 * }
 * 模式匹配有返回值,返回值就是符合条件的分支的块表达式的结果值
 */
object $01_MatchDefined {
  def main(args: Array[String]): Unit = {
    val list = List(1,10,3.3,"hello",false,true,"spark")

    val index = Random.nextInt( list.length )
    println(index)

    val res = list(index) match {
      case 1 =>  // =>右边的{}可以省略
        println("1,....")
        1
      case 3.3 =>
        println("3.3,,.....")
        2
      case "spark"=>
        println("spark,....")
        3
      case "hello" =>
        println("hello, ...")
        4
      case false=>
        println("false.....")
        5
//      case x=>
//        println(s"${x}......")
      case _=>  //  //如果x变量在=>右边不使用,可以使用_代替
        println("other....")
        -1
    }
    println(res)


  }
}
