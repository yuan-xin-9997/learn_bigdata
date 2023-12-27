package com.atguigu.chapter06

object $06_ObjectClass {

  /**
   * 伴生类[class]与伴生对象[object]
   *    1、class与object名称必须一样
   *    2、class与object必须在同一个.scala源文件中
   *
   * 伴生类和伴生对象可以互相访问对方的private修饰的成员
   *
   * apply方法：必须定义在object中，主要用于简化对象的创建
   * @param args
   */
  def main(args: Array[String]): Unit = {
    println(Animal.age)
    println(new Animal().name)
    println("====================")
    // 伴生类和伴生对象可以互相访问对方的private修饰的成员
    println(new Animal().name)
    new Animal().getSex()
    Animal.getColor()
    println("====================")
    val animal = Animal.apply()
    println(animal.name)
    val animal1 = Animal()
    println(animal1.name)
  }
}


class Animal{
  val name = "xxx"
  private val color = "red"
  def getSex(): Unit = {
    println(Animal.sex)
  }
}

object Animal{
  val age = 20;
  private val sex = "man"
  def getColor(): Unit = {
    println(new Animal().color)
  }

  def apply() = {
    new Animal()
  }
}