package com.atguigu.chapter07collection

import scala.collection.mutable.ListBuffer

object $04_MutableList {
  def main(args: Array[String]): Unit = {
    // 可变List创建方式1:ListBuffer[元素类型](初始元素,...)
    val list = ListBuffer[Int](1, 4, 3, 2)
    println(list)

    // 可变List创建方式2:new ListBuffer[元素类型]
    val list1 = new ListBuffer[Int]
    println(list1)

    //  增加元素
    var list3=ListBuffer[Int](4,5,6)
    println(list3.hashCode())

    list3+:=(40)  // 等价于 list3=list3.+:(40)
    println(list3.hashCode())
    println(list3)

    // 添加、删除、与可变数组一致

    // 获取指定元素
    println(list(0))

    //  修改元素
    list(0)=1000
    println(list)


  }
}
