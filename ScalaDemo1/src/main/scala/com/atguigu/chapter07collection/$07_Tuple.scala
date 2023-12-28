package com.atguigu.chapter07collection

object $07_Tuple {

  /**
   * 元素
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 元素创建
    //    方式1：通过()的形式创建，(初始元素,...)   可以用于创建任何元素个数的元组
    //    方式2：通过->方式创建，K->V   只能用于创建二元元组
    // 元组最多只能存放22个元素
    // 元组一旦创建，元素个数、元素都不可变
    // 元素获取值的方式：._N

    val t1=("zhangsan", 20, "lisi")
    println(t1)

    val t2= "张三"->20
    println(t2)

    println(t1._1)
    println(t1._2)
    println(t1._3)
    // println(t1._4)

    // 使用元组封装数据
    val list = List("1 张三 20 Beijing", "2 张三 20 Beijing", "3 张三 20 Beijing", "4 张三 20 Beijing")
    val list2= for (elem <- list) yield {
      val arr = elem.split(" ")
      val name = arr(1)
      val age = arr(2).toInt
      (name, age)
    }
    println(list2)

    // 元素封装数据的缺点：当数据属性多的时候，不便于阅读，代码可读性差
    val list3 = List(
      new Region("宝安区", new School("保安中学", new Clazz("王者峡谷班", new Student("安其拉", age=10)))),
      new Region("宝安区", new School("保安中学", new Clazz("王者峡谷班", new Student("安其拉", age=11)))),
      new Region("宝安区", new School("保安中学", new Clazz("王者峡谷班", new Student("安其拉", age=12)))),
      new Region("宝安区", new School("保安中学", new Clazz("王者峡谷班", new Student("安其拉", age=13))))
    )
    val stuAges = for (region <- list3) yield {
      region.school.clazz.stu.age
    }
    println(stuAges)

    val list4 = List(
      ("宝安区", ("保安中学", ("王者峡谷班", ("安其拉", 10)))),
      ("宝安区", ("保安中学", ("王者峡谷班", ("安其拉", 11)))),
      ("宝安区", ("保安中学", ("王者峡谷班", ("安其拉", 12)))),
      ("宝安区", ("保安中学", ("王者峡谷班", ("安其拉", 13))))
    )
    val stuAges1 = for (region <- list4) yield {
      region._2._2._2._2
    }
    println(stuAges1)

  }

  class Region(val name:String, val school:School)
  class School(val name:String, val clazz:Clazz)
  class Clazz(val name:String, val stu:Student)
  class Student(val name:String, val age:Int)



}
