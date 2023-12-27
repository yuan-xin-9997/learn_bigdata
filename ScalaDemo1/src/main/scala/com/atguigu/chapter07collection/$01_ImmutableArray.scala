package com.atguigu.chapter07collection

object $01_ImmutableArray {

  // 不可变数组 Array

  /**
   * 集合通用的添加、删除元素方法的区别
   *    + -
   *      + 添加元素
   *      - 删除元素
   *    一个+/-与两个+/-的区别
   *      一个+/- 添加或删除单个元素
   *      两个+/- 添加或删除一个集合所有元素
   *    冒号在前、冒号在后、不带冒号
   *       冒号在前、不带冒号，将元素添加到集合最后面
   *       冒号在后将元素添加到集合最前面
   *    带=与不带=区别
   *        不带=是添加、删除元素的时候，原集合不变，生成新集合
   *        带=是向原集合中添加、删除元素
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 创建数组方式1：new Array[元素类型](长度)
    val arr = new Array[Int](5)
    println(arr)
    println(arr.toList)
    // 创建数组方式2：使用apply方法创建：Array[元素类型](初始元素,...)常用
    val arr2 = Array[Int](1, 2, 3, 4)
    println(arr2.toList)

    println(arr)

    // 添加元素（不可变数组，添加元素，将返回新的数组）
    val arr3 = arr.+:(10)
    println(arr3.toList)
    println(arr.toList)

    val arr4 = arr.:+(20)
    println(arr3.toList)
    println(arr4.toList)

    val arr5 = arr.++(Array(1, 2, 3, 4))
    println(arr5.toList)
    println(arr.toList)

    val arr6 = arr.++:(Array(1, 2, 3, 4))
    println(arr6.toList)
    println(arr.toList)

    // 删除元素（不可变数组不可以删除元素）

    // 获取指定元素
    println(arr6(0))

    // 修改元素（没有改变长度）
    arr6(0)=666
    println(arr6.toList)
  }
}
