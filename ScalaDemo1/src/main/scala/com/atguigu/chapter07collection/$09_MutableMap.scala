package com.atguigu.chapter07collection

import scala.collection.mutable

object $09_MutableMap {
  /**
   * 可变Map
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 可变Map的创建
    // mutable.Map[K的类型, V的类型]=(K->V, (K,V), ...)
    val map = mutable.Map[String, Int]()
    println(map)

    // 添加元素  put remove都是类似Java Map的API方法
    map.put("cc", 40)
    println(map)

    println(map.getOrElse("cc", 40))
    println(map.getOrElse("aa", -1))

    // 删除元素
    map.remove("cc")
    println(map)


    // 更新元素
    map.put("aa", 1)
    map("aa")=2
    println(map)
    map.update("aa", 3)
    println(map)

  }
}
