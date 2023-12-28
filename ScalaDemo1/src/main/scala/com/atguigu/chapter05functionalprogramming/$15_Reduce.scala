package com.atguigu.chapter05functionalprogramming

object $15_Reduce {

  /**
   * 4、按照指定规则对集合所有元素进行聚合
   * 数据: Array(10,30,20,50)
   * 规则: 求和[动态]
   * 结果: 110
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val arr = Array(10,30,20,50)
    println(reduce(arr))

    val  func =(x:Int, y:Int)=>x+y
    println(reduce1(arr, func))
    // 函数简化
    println(reduce1(arr, (x:Int, y:Int)=>x+y))
    println(reduce1(arr, (x, y)=>x+y))
    println(reduce1(arr, _+_))
  }

  def reduce(arr:Array[Int])={
    var tmp = arr(0)
    for (elem <- 1 until arr.length) {
      tmp = tmp  + arr(elem)
    }
    tmp
  }

  def reduce1(arr: Array[Int], func:(Int, Int)=>Int) = {
    var tmp = arr(0)
    for (elem <- 1 until arr.length) {
      tmp = func(tmp, arr(elem))
    }
    tmp
  }

}
