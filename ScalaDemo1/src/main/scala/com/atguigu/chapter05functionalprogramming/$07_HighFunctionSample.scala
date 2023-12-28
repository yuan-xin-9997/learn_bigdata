package com.atguigu.chapter05functionalprogramming

/**
 * 高阶函数简化：
 *    1、可以直接传递函数值
 *    2、可以省略函数的参数类型
 *    3、函数如果只有一个参数，参数列表的()可以省略
 *    4. 函数的参数如果在参数体中只使用了一次可以用_代替，第N个下划线代表函数第N个参数
 *      以下三种情况不能用_简化
 *        1. 针对函数有多个参数的情况，函数的参数的使用顺序与定义顺序不一致，不能用_简化
 *        2. 针对函数中有()的情况，并且函数的参数在函数体()中以表达式存在
 *        3. 针对函数只有一个参数，并且在函数体中没有任何操作直接返回函数的参数，不可简化。否则无任何反应
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

//    针对函数有多个参数的情况，函数的参数的使用顺序与定义顺序不一致，不能用_简化
    println(add(10,20, (x,y)=>y-x))
    println(add(10,20, _-_))

    // 针对函数中有()的情况，并且函数的参数在函数体()中以表达式存在
    println(add(10, 20, (x, y) => (x + 1) * y))
    // missing parameter type for expanded function ((x$6: <error>) => ((<x$5: error>: <error>) => <x$5: error>.<$plus: error>(1)).<$times: error>(x$6))
    //    println(add(10, 20, (_ + 1) * _))
    // println(add(10, 20, (_ + 1) * _))
    println(20.$times(10).$plus(10))

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
