package com.atguigu.chapter05functionalprogramming

object $13_Filter {

  /**
   * 2、按照指定的规则对集合数据进行过滤
   * 数据: Array(1,4,3,6,7,9,10)
   * 规则: 保留偶数[动态]
   * 结果: Array(4,6,10)
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val arr= Array(1,4,3,6,7,9,10)

    // println(filter(arr).toList)

    val func=(x:Int)=>x%2==0
    val func2=(x:Int)=>x%2!=0
    println(filter(arr,func).toList)
    println(filter(arr,func2).toList)
    println(filter(arr, (x:Int)=>x%2!=0).toList)
    // 高阶函数省略
    println("高阶函数省略")
    println(filter(arr, (x)=>x%2!=0).toList)
    println(filter(arr, x=>x%2!=0).toList)
    println(filter(arr, _%2!=0).toList)
  }

//  def filter(arr:Array[Int])={
//    for (elem <- arr ) yield {
//      if(elem%2==0)  {
//        elem
//      }else{}
//    }
//  }

//  def filter(arr: Array[Int]) = {
//    for (elem <- arr if (elem % 2 == 0)) yield {
//      elem
//    }
//  }

  def filter(arr: Array[Int], func:Int=>Boolean) = {
    for (elem <- arr if (func(elem))) yield {
      elem
    }
  }


}
