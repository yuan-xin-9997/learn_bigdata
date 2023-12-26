package com.atguigu.chapter05

object $08_NoNameFunction {
  /**
   * 说明
     * 没有名字的函数/方法就是匿名函数。
     * (x:Int)=>{函数体}
     * x：表示输入参数类型；Int：表示输入参数类型；函数体：表示具体代码逻辑
   *
   *  匿名函数用于给高阶函数传值使用
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val func=(x:Int, y:Int)=>x+y
    println(func.apply(10, 20))

    // 使用匿名函数
    ((x:Int, y:Int)=>x+y)(10, 20)

    println(m1(10, 20, (x: Int, y: Int) => x + y))
  }


  def m1(x:Int, y:Int, func: (Int, Int)=>Int)={
    func.apply(x, y)
  }







}
