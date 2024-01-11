package com.atguigu.day05

import scala.collection.immutable.Nil

object test {
  def main(args: Array[String]): Unit = {
    val a: (Int, (Int, Int, Int)) = (1, (1, 0, 0))
    println(a)
    println(a.getClass.getSimpleName)

    val b: List[(Int, (Int, Int, Int))] = a :: Nil
    println(b)
    println(b.getClass)

    val list3 = List(List(1, 2, 3), List(4, 5, 6, List(7, 8, 9)))
    println(list3.flatten)

    val list4 = List((1, 2, 3), List(4, 5, 6, List(7, 8, 9)))
    // println(list4.flatten)
  }
}
