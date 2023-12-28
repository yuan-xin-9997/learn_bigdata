package com.atguigu.chapter02datatype

object Test03_Type {
  def main(args: Array[String]): Unit = {
    // 所有的代码都是代码块
    // 表示运行一段代码  同时将最后一行的结果作为返回值
    // 千万不要写return
    val i: Int = {
      println("我是代码块")
      10 + 10
    }

    // 代码块为1行的时候  大括号可以省略
    val i1: Int = 10 + 10

    // 如果代码块没有计算结果  返回类型是unit
    val unit: Unit = {
      println("hello")
      println("我是代码块")
    }


    // 当代码块没办法完成计算的时候  返回值类型为nothing
    //    val value: Nothing = {
    //      println("hello")
    //      throw new RuntimeException
    //    }

    val l = 1L

    // （1）Scala各整数类型有固定的表示范围和字段长度，不受具体操作的影响，以保证Scala程序的可移植性。
    val b1: Byte = 2
    //    val b0: Byte = 128

    //val b2: Byte = 1 + 1
    //println(b2)

    val i2 = 1

    //（2）编译器对于常量值的计算 能够直接使用结果进行编译
    // 但是如果是变量值 编译器是不知道变量的值的 所以判断不能将大类型的值赋值给小的类型
    //    val b3: Byte = i2 + 1
    //    println(b3)

    // （3）Scala程序中变量常声明为Int型，除非不足以表示大数，才使用Long
    val l1 = 2200000000L

    // 浮点数介绍
    // 默认使用double
    val d: Double = 3.14

    // 如果使用float 在末尾添加f
    val fl = 3.14f

    // 浮点数计算有误差
    println(0.1 / 3.3)

    println("-------------------------------")

    //    （1）字符常量是用单引号 ' ' 括起来的单个字符。
    val c1: Char = 'a'
    val c2: Char = 65535

    //    （2）\t ：一个制表位，实现对齐的功能
    val c3: Char = '\t'

    //    （3）\n ：换行符
    val c4: Char = '\n'
    println(c3 + 0)
    println(c4 + 0)

    //    （4）\\ ：表示\
    val c5: Char = '\\'
    println(c5 + 0)

    //    （5）\" ：表示"
    val c6: Char = '\"'
    println(c6 + 0)

    println("-------------------------------")
    val bo1: Boolean = true
    val bo2: Boolean = false
    print(bo1)
    println(bo2)

    println("-------------------------------")
    // unit
    val unit1: Unit = {
      10
      println("1")
    }
    println(unit1)

    // 如果标记对象的类型是unit的话  后面有返回值也没法接收
    // unit虽然是数值类型  但是可以接收引用数据类型   因为都是表示不接收返回值
//    val i4: Unit = "aa"
//    println(i4)

    println("-------------------------------")
    // unit
    val unit2: Unit = {
      10
      println("1")
    }
    println(unit2)

    // 如果标记对象的类型是unit的话  后面有返回值也没法接收
    // unit虽然是数值类型  但是可以接收引用数据类型   因为都是表示不接收返回值
    val i3: Unit = "aa"
    println(i3)

    // scala当中使用的字符串就是java中的string
    val aa: String = "aa"


    println("-"*100)
    // null
    var aa1: String = "aa"
    println(aa1)

    aa1 = "bb"
    aa1 = null
    if (aa1 != null) {
      val strings: Array[String] = aa1.split(",")
    }

    // 值类型不能等于null,idea不会识别报错  编译器会报错
    var i4 = 10
    //    i4 = null
  }
}
