package com.atguigu.chapter05functionalprogramming


/**
 * 高阶函数：以函数作为参数/返回值的方法或函数，称之为高阶函数
 *
 * 函数式编程：
 *  遍历（foreach）
    映射（map）
    映射扁平化（flatmap）
    过滤（filter）
    是否存在（exists）
    排序（sorted、sortBy、sortWith）
    分组（groupBy）
    聚合计算（reduce）
    折叠（fold）
 */
object $06_HighFunction {
  def main(args: Array[String]): Unit = {

    // 定义一个函数
//    val func = new Function2[Int, Int, Int] {
//      override def apply(v1: Int, v2: Int): Int = v1 + v2
//    }
    val func=(x:Int,y:Int)=>x+y

    println(add(10, 20, func))
  }

  // 高阶函数举例，传入的函数类似于java里面方法传参是一个对象
  // def add(x:Int, y:Int, func: Function2[Int,Int,Int])
  def add(x:Int, y:Int, func: (Int, Int)=>Int) = {
    func.apply(x, y)
    // func(x, y)
  }
}
