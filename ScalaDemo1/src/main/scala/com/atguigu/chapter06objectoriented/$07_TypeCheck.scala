package com.atguigu.chapter06objectoriented

import scala.util.Random

object $07_TypeCheck {

  class Animal

  class Dog extends Animal{
    val color = "red"
  }

  class Pig extends Animal{
    val weight = 20
  }

  def getAnimal()={
    val index =  Random.nextInt(10)
    if(index%2==0){new Pig}
    else{new Dog}
  }

  /**
   * Java中判断对象是否属于某个类型，对象 instanceof 类型
   * Java中将对象强制转换成某个类型： (类型)对象
   * Java中获取对象的class形式：对象.getClass
   * Java中获取类的class形式：类名.class
   *
   * Scala中判断对象是否属于某个类型：对象.isInstanceOf[类型]
   * Scala中将对象强制转换成某个类型：对象.asInstanceOf[类型]
   * Scala中获取对象的class形式：对象.getClass
   * Scala中获取类的class形式：classOf[类名]
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val animal:Animal = getAnimal()

    if(animal.isInstanceOf[Pig]){
      val pig = animal.asInstanceOf[Pig]
      println(pig.weight)
    }else{
      val dog = animal.asInstanceOf[Dog]
      println(dog.color)
    }

    println(classOf[Pig])
    println(classOf[Dog])
    println(animal.getClass)

  }
}
