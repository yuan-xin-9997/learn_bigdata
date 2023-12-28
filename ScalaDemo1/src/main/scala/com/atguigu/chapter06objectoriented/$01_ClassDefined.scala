package com.atguigu.chapter06objectoriented

object $01_ClassDefined {
  /**
   * Scala的面向对象思想和Java的面向对象思想和概念是一致的。
   * Scala中语法和Java不同，补充了更多的功能。
   *
   * java 定义类 [访问修饰符] class 类名{...}
   * java 创建对象 new 类名(...)
   *
   * scala 定义类 [访问修饰符] class 类名{...}
   * scala 创建对象 new 类名(...)
   * @param args
   */

    class Person

  def main(args: Array[String]): Unit = {
    val person = new Person
    println(person)
  }
}
