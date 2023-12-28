package com.atguigu.chapter05functionalprogramming

object $09_Currie {

  /**
   * 函数柯里化：将一个接收多个参数的函数转化成一个接受一个参数的函数过程，可以简单的理解为一种特殊的参数列表声明方式。
   * 闭包：就是一个函数和与其相关的引用环境（变量）组合的一个整体(实体)
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    println(sum(10, 20)(30)(40))
    println(add(10, 20, 30,40))

    val func=sum2(10, 20)
    val func2=func(30)
    val r=func2(40)
    println(r)

    println(sum2(10, 20).apply(30).apply(40))
    println(sum2(10, 20)(30)(40))
  }


  def sum(x:Int,y:Int)(z:Int)(a:Int):Int=x+y+z+a

  def add(x:Int, y:Int, z:Int, a:Int)=x+y+z+a

  def sum2(x:Int, y:Int)={
    val func=(z:Int)=>{
      val f=(a:Int)=>{
        x+y+z+a
      }
      f
    }
    func
  }



}
