package com.atguigu.chapter05

/**
 * 方法和函数的区别:
 *    1. 方法如果定义在class/object中可以重载，函数式对象，函数就是变量名，同一作用域不允许有同名的变量，因此，函数不可以重载
 *    2. 方法存储在方法区中，函数式对象，保存在堆中
 *
 *  方法和函数的联系：
 *    1. scala中方法可以定义在方法里面（此时不可以重载，其实方法此时就是函数），但是java不行
 *    2. 方法可以手动转换成函数，使用方法名 _ 转换
 */
object $05_MethodAndFunction {
  def main(args: Array[String]): Unit = {

    // scala中方法可以定义在方法里面，但是java不行
    def m2(x:Int)=x*10
    println(m2(10))
    // 方法内部的方法不可以重载
    // def m2(x:Int,y:Int)=x*y // m2 is already defined in the scope

    // 方法转换成函数：方法可以手动转换成函数，使用方法名 _ 转换
    val f = m2 _
    println(f(5))
  }

  // 方法重载
  def m1(x:Int)=x*x
  def m1(x:Int,y:Int)=x*y

  // 函数不可以重载
  val func=(x:Int)=>x*x
  // val func=(x:Int,y:Int)=>x*y // func is already defined in the scope
}
