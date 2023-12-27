package com.atguigu.chapter07collection

object $05_ImmutableSet {
  def main(args: Array[String]): Unit = {

    // set 元素不重复、无序、无索引
    // 不可变Set的创建：Set[元素类型](初始元素,...)
    val s1 = Set(1,10,4,3,2,1)
    println(s1)

    // 添加元素
    val s2 = s1.+(20)
    println(s2)

    val s3 = s1.++(List(11, 44, 23, 33))
    println(s3)

    val s4 = s1.++:(List(11, 44, 23, 33))
    println(s4)

    // 删除元素
    println(s1)
    val s5 = s1.-(1)
    println(s5)

    val s6 = s1.--(List(2, 3, 4))
    println(s6)

    // set 可以获取角标元素
  }
}
