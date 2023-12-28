package com.atguigu.chapter05functionalprogramming

object $12_Map {

  /**
   * 1、按照指定的规则获取集合中每个元素操作之后返回结果
   * 数据: Array("hello","hadoop","flume","spark","kafka")
   * 规则: 获取每个元素的长度[动态]
   * 结果: Array(5,6,5,5,5)
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val arr = Array("hello", "hadoop", "flume", "spark", "kafka")

    // println(map(arr).toList)
    val func=(x:String)=>x.length
    println(map(arr, func).toList)

    val func1=(x:String)=>x.charAt(0)
    println(map(arr, func1).toList)

    // 直接传递函数值
    println(map(arr, (x:String)=>x.length).toList)
    // 省略函数参数类型
    println(map(arr, (x)=>x.length).toList)
    // 简化函数
    println(map(arr, x=>x.length).toList)
    println(map(arr, _.length).toList)
  }

  def map(arr:Array[String], func:(String)=>Any)={
    for (elem <- arr) yield {
      // elem.length
      // elem.charAt(0)
      func(elem)
    }
  }
}
