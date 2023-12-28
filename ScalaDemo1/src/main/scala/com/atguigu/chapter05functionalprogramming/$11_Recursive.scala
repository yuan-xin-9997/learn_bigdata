package com.atguigu.chapter05functionalprogramming

object $11_Recursive {

  /**
   * 一个函数/方法在函数/方法体内又调用了本身，我们称之为递归调用
   * // 阶乘
   * // 递归算法
   * // 1) 方法调用自身
   * // 2) 方法必须要有跳出的逻辑
   * // 3) 方法调用自身时，传递的参数应该有规律
   * // 4) scala中的递归必须声明函数返回值类型
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    println(m1(5))
  }

  // 递归函数
  def m1(n:Int):Int={
    if(n==1)
      1
    else
      n * m1(n-1)
  }
}
