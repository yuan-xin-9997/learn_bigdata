package com.atguigu.chapter08match

import scala.util.Random

object $05_PartialFunction {

  /**
   * 偏函数: 没有match关键字的模式匹配称之为偏函数
   * 语法:
       * val 函数名: PartialFunction[IN,OUT] = {
       * case 条件1 => ....
       * case 条件3 => ....
       * case 条件2 => ....
       * .....
       * }
   * 或者
   * val 函数名: IN => OUT = {
       * case 条件1 => ....
       * case 条件3 => ....
       * case 条件2 => ....
       * .....
   * }
   *
   * 偏函数是一个函数,该函数有且仅有一个参数,IN就是函数参数类型,OUT就是函数的返回值类型
   */
  def main(args: Array[String]): Unit = {

    val list = List(1,10,3.3,"hello",false,true,"spark")

    val index = Random.nextInt(list.length)
    println(index)

    //TODO 定义偏函数,从数组中取值匹配该值
//    val func:Any =>Int={
    val func:PartialFunction[Any,Int]={
      case 1 =>
        println("1.....")
        10
      case 3.3 =>
        println("3.3.....")
        20
      case "spark" => // =>右边的{}可以省略
        val a = 10
        val b = 30
        println(a + b)
        println("spark.....")
        30
      case "hello" =>
        println("hello.....")
        40
      case false =>
        println("false....")
        50
      //case x =>  println(s"${x}.....+") //相当于switch的default
      case _ =>
        println(s"其他.....+") //如果x变量在=>右边不使用,可以使用_代替
        -1
    }
    func(list(index))

    println("----------")
    // TODO 偏函数在工作中一般是集合元组匹配使用,增强代码的可读性
    val list4 = List(
      ("宝安区", ("宝安中学", ("王者峡谷班", ("安其拉", 18)))),
      ("宝安区", ("宝安中学", ("王者峡谷班", ("甄姬", 18)))),
      ("宝安区", ("宝安中学", ("王者峡谷班", ("妲己", 18)))),
      ("宝安区", ("宝安中学", ("王者峡谷班", ("王昭君", 18)))),
      ("宝安区", ("宝安中学", ("王者峡谷班", ("扁鹊", 18))))
    )
    // 定义偏函数
//    val func2: (    (String, (String, (String, (String, Int))))    )=>String={
//      case (regionName, (schoolName, (clazzName, (name, age)))) => name
//    }
    val func2: PartialFunction[    (String, (String, (String, (String, Int)))),String]={
      case (regionName, (schoolName, (clazzName, (name, age)))) => name
    }
    // 取出学生姓名
    println(list4.map(func2))

    // TODO 工作中的偏函数的使用
    val list5 = list4.map {
      case (regionName, (schoolName, (clazzName, (name, age)))) => name
    }
    println(list5)


  }
}
