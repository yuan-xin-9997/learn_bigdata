package com.atguigu.chapter07collection

import scala.collection.mutable.ArrayBuffer

object $02_MutableArray {

  /**
   * 可变数组
   * 集合通用的添加、删除元素方法的区别
   * + -
   *      + 添加元素
   *      - 删除元素
   * 一个+/-与两个+/-的区别
   *        一个+/- 添加或删除单个元素
   *        两个+/- 添加或删除一个集合所有元素
   *  冒号在前、冒号在后、不带冒号
   *        冒号在前、不带冒号，将元素添加到集合最后面
   *        冒号在后将元素添加到集合最前面
   *  带=与不带=区别
   *        不带=是添加、删除元素的时候，原集合不变，生成新集合
   *        带=是向原集合中添加、删除元素
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 可变数组创建方式一：不指定初始元素类型
    val arr1 = new ArrayBuffer[Int]()
    // 可变数组创建方式二：指定初始元素类型
    val arr2 = ArrayBuffer[Int](1, 2, 6, 4)

    // 可变数组重写了toString方法，可以直接打印数组元素
    println(arr1)
    println(arr2)

    // 添加元素
    val arr3 = arr2.+:(10)
    println(arr3)

    val arr4 = arr2.:+(20)
    println(arr4)

    arr2.+=(30)
    println(arr2)

    arr2.+=:(40)
    println(arr2)

    val arr5= arr2.++(Array(11,22,33))
    println(arr5)

    val arr6 = arr2.++:(Array(11, 22, 33))
    println(arr6)

    arr2.++=(Array(111,222,111))
    println(arr2)

    arr2.++=:(Array(666,777,888))
    println(arr2)

    // 删除元素
    val arr7 = arr2.-(111)
    println(arr7)

    arr2.-=(666)
    println(arr2)

    val arr8 = arr2.--(Array(4, 1, 3, 111, 111, 222))
    println(arr8)

    arr2.--=(Array(777,888))
    println(arr2)

    // 获取角标元素
    println(arr2(0))

    // 修改元素值
    arr2(0)=111111
    println(arr2)

    // 遍历
    for (elem <- arr2) {
      println(elem)
    }

    // 不可变转可变
    val arr10 = Array(1, 2, 3)
    println(arr10)
    println(arr10.toBuffer)

    // 创建多维数组
    val arr11 = Array.ofDim[Int](4, 3)
    println(arr11.length)
    println(arr11(0).length)
  }

}
