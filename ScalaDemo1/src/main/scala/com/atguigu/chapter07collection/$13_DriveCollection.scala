package com.atguigu.chapter07collection

object $13_DriveCollection {

  /**
   * 衍生集合
   * @param args
   */
  def main(args: Array[String]): Unit = {


    val list  = List(1,2,3,5,7,10,1,1,1,1)

    println(list)

    // 元素去重
    println(list.distinct)

    // 删除前3个元素
    println(list.drop(3))

    // 删除后3个元素
    println(list.dropRight(3))

    // 取前3个元素
    println(list.take(3))

    // 取后3个元素
    println(list.takeRight(3))

    // 分区获取元素，slice(i,j)，获取从i到j-1的元素
    println(list.slice(1, list.size - 1))

    // 获取第一个元素
    println(list.head)

    //获取最后一个元素
    println(list.last)

    // 获取除第1个元素外的所有元素
    println(list.tail)

    // 获取除最后1个元素外的所有元素
    println(list.init)

    // 反转
    println(list.reverse)

    // 滑窗、滑动窗口
    println(list.sliding(2).toList)
    println(list.sliding(2,1).toList)
    println(list.sliding(3,2).toList)

    // 交集
    val list2 = List(1,2,3,4,5)
    val list3 = List(4,5,6,7,8)
    println(list2.intersect(list3))

    // 差集
    println(list2.diff(list3))

    // 并集，不会去重
    println(list2.union(list3))

    // 拉链
    val list4 = List("aa", "bb", "cc", "dd", "ee")
    val list5 = List(10,20,30,40)
    println(list4.zip(list5))

    // 反拉链
    val list6 = list4.zip(list5)
    println(list6.unzip)

    println(list4.zipWithIndex)

    println("-------------------------------------------")
    println(list)

  }
}
