package com.atguigu.chapter05

/**
 * 高阶函数简化：
 *    1、可以直接传递函数值
 *    2、可以省略函数的参数类型
 *    3、函数如果只有一个参数，参数列表的()可以省略
 *    4. 函数的参数如果在参数体中只使用了一次可以用_代替
 */
object $07_HighFunctionSample {
  def main(args: Array[String]): Unit = {

    val f = (a:Int, b:Int)=>a+b
    println(add(10, 20, f))
    // 1、可以直接传递函数值
    println(add(10, 20, (a:Int, b:Int)=>a+b))
    // 2、可以省略函数的参数类型
    println(add(10, 20, (a, b)=>a+b))
    // 4. 函数的参数如果在参数体中只使用了一次可以用_代替
    println(add(10, 20, _+_))


    val fc=(x:String)=>{
      println(s"hello:${x}")
    }
    sayHello("zhangsan", fc)
    // 1、可以直接传递函数值
    sayHello("zhangsan", (x:String)=>println(s"hello:${x}"))
    // 2、可以省略函数的参数类型
    sayHello("zhangsan", (x)=>println(s"hello:${x}"))
    // 3、函数如果只有一个参数，参数列表的()可以省略
    sayHello("zhangsan", x=>println(s"hello:${x}"))

  }

  // 定义一个高阶函数
  def add(x:Int, y:Int, func:(Int,Int)=>Int) = {
    func(x, y)
  }

  def sayHello(msg:String, func:(String)=>Unit)={
    func(msg)
  }

}
