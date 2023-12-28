package com.atguigu.chapter05functionalprogramming

object $16_MaxBy {

  /**
   * 5、按照指定规则获取集合中最大元素
   * 数据: Array("zhangsan 20 4500","lisi 33 6800","hanmeimei 18 10000")
   * 规则: 获取年龄最大的人
   * 结果: "lisi 33 6800"
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val arr = Array("zhangsan 20 4500","lisi 33 6800","hanmeimei 18 10000")
    println(maxBy(arr))
    val func=(x:String)=>x.split(" ")(1).toInt
    println(maxBy1(arr, func))
    println(maxBy1(arr, (x:String)=>x.split(" ")(2).toInt))
    // 函数简化
    println(maxBy1(arr, (x)=>x.split(" ")(2).toInt))
    println(maxBy1(arr, x=>x.split(" ")(2).toInt))
    println(maxBy1(arr, _.split(" ")(2).toInt))
  }

  def maxBy(arr:Array[String])={
    var tmp=arr(0).split(" ")(1).toInt
    var tmpEle=arr(0)
    for (elem <- arr) {
      val age = elem.split(" ")(1).toInt
      if(age>tmp){
        tmp=age
        tmpEle=elem
      }
    }
    tmpEle
  }

  def maxBy1(arr: Array[String], func:String=>Int) = {
    var tmp = func(arr(0))
    var tmpEle = arr(0)
    for (elem <- arr) {
      val age = func(elem)
      if (age > tmp) {
        tmp = age
        tmpEle = elem
      }
    }
    tmpEle
  }


}
