package com.atguigu.chapter07collection

object $12_CollectionFields {
  /**
   * 集合常见属性
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val list = List(1, 4, 2, 4, 5)

    // 获取集合长度
    println(list.length)
    println(list.size)

    // 判断集合是否为空
    println(list.isEmpty)

    // 判断集合是否包含某个元素
    println(list.contains(4))

    // 将集合拼接为字符串
    println(list.mkString("#"))

    // 集合转迭代器
    val it = list.toIterator
    println(it)
    while (it.hasNext) {
      println(it.next())
    }

    // 可重复使用的迭代器
    val it2 = list.toIterable
    println(it2)
    for (elem <- it2) {
      println(elem)
    }
    for (elem <- it2) {
      println(elem)
    }


  }
}
