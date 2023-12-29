package com.atguigu.chapter02

object Test04_TypeCast {

  def main(args: Array[String]): Unit = {

    //    （1）自动提升原则：有多种类型的数据混合运算时，
    //    系统首先自动将所有数据转换成精度大的那种数据类型，然后再进行计算。

    val fl: Float = 1 + 1L + 3.14f
    val d: Double = 1 + 1L + 3.14f + 3.14

    //    （2）把精度大的数值类型赋值给精度小的数值类型时，就会报错，反之就会进行自动类型转换。
    val i = 10
    val b: Double = i

    //    （3）（byte，short）和char之间不会相互自动转换。
    // 因为byte和short是有符号的数值,而char是无符号的
    val b1: Byte = 10
    val c1: Char = 20

    //    （4）byte，short，char他们三者可以计算，在计算时首先转换为int类型。
    val b2: Byte = 20
    //    val i1: Byte = b1 + b2
    val i1: Int = 1100000000
    val i2: Int = 1200000000
    // 超出范围的int值计算会造成结果错误
    val i3: Int = i1 + i2
    println(i3)

  }

}
