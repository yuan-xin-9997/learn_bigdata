package com.atguigu.chapter05functionalprogramming

/**
 * 函数的定义语法：
 *  val/var 函数名[:类型]=(参数名:类型)=>{函数体}
 *  函数简化，如果函数体只有一行语句，可以省略{}
 *  scala：方法就是函数，函数也是对象（函数是匿名子类对象）
 *
 *  函数是一个对象（变量）
    类似于方法，函数也有输入参数和返回值
    函数定义不需要使用def定义
    无需指定返回值类型

    同一个包下不可以有两个同名的类
 */
object $04_FunctionDefined {
  def main(args: Array[String]): Unit = {

    println(add(10, 30))
    println(add1(10, 30))

    println(add1) // 打印函数的内存地址
    println(func)  // 打印函数的内存地址

    println(add2(10, 20))
    println(add1.apply(10, 20))
    println(func.apply)

    val f = func
    println(f())

  }

  // 定义一个函数，有两个Int类型参数，函数的返回值为Int类型
//  val add: (Int, Int) => Int = (x:Int, y:Int)=>{
//    x+y
//  }
  val add: Function2[Int, Int, Int] = (x:Int, y:Int) => x + y

  // 函数简化，如果函数体只有一行语句，可以省略{}
  val add1 = (x: Int, y: Int) => x + y
  val func=()=>println("hello....")

  // 函数是对象，因此在scala中函数可以作为方法的参数进行传递，java中则不可以
  val add2 = new Function2[Int, Int, Int]{
    override def apply(v1: Int, v2: Int): Int = v1+v2
  }


}
