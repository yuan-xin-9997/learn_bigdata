package com.atguigu.chapter08match

import scala.util.Random

object $02_MatchType {
  class Person
  class Student
  /**
   * scala中match不光可以匹配值，也可以匹配类型
   * 匹配类型语法
   * 变量 match {
       * case x:类型1 => ...
       * case x:类型2 => ...
       * case x:类型3 => ...
   * }
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val list = List(1,10,3.3,"hello",false,true,"spark",new Person,new Student)
    val index = Random.nextInt(list.length)
    println(index)

    list(index) match{
      case x:Int=>
        println(s"int.......${x}")
      case x:Double=>
        println(s"double.......${x}")
      case x:Person=>
        println(s"Person.......${x}")
      case x:Student=>
        println(s"Student.......${x}")
      case _:Boolean=>
        println(s"boolean.......")
      case x=>
        println("其他类型")
    }


  }
}
