package com.atguigu.chapter07collection

import scala.collection.mutable

object $06_MutableSet {
  def main(args: Array[String]): Unit = {


    // 可变Set
    // 创建方式：mutable.Set[元素类型](初始元素...)
    val s1 = mutable.Set[Int](1, 4, 3, 2, 1, 5)
    println(s1)

    // 添加元素
    val s2 = s1.+(10)
    println(s2)

    s1.+=(20)
    println(s1)

    val s3 = s1.++:(List(11, 44, 22, 33))
    val s4 = s1.++(List(11, 44, 22, 33))
    println(s3)
    println(s4)

    s1.++=(List(66,77,88))
    println(s1)

    // 删除元素
    println(s1)
    val s5 = s1.-(1)
    println(s5)

    s1.-=(66)
    println(s1)

    val s6 = s1.--(List(5, 2, 3, 4))
    println(s6)

    s1.--=(List(66,4,3))
    println(s1)

  }
}
