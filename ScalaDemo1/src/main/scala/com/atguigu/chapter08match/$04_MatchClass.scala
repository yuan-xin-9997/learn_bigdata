package com.atguigu.chapter08match

object $04_MatchClass {

  // 创建样例类
  case class Person(val name:String, var age:Int, address:String)
  class Student1(val name:String, var age:Int, address:String)

  /**
   * 匹配样例类
   * 样例类: 其实就是伴生类与伴生对象的封装
       * 语法: case class 类名( [val/var] 属性名:类型[=默认值],.... )
       * 样例类中属性如果没有用val/var修饰,默认就是val修饰的
       * 创建样例类对象: 类名(属性值,...) / 类名.apply(属性值....)
   *
   * 样例类可以直接用于模式匹配
   * 普通类不能直接用于模式匹配,如果想要用于模式匹配需要在伴生对象中定义unapply方法
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val person = Person("lisi", 20, "shenzhen")
    println(person)  // 样例类重写了toString方法，print的不是地址，而是Person(lisi,20,shenzhen)
    println(person.name)
    println(person.age)
    println(person.address)
    // 样例类中属性如果没有用val/var修饰,默认就是val修饰的
    // person.address = "beijing"  // Reassignment to val
    person.age = 10
    println(person.age)

    val student = new Student1("lisi", 20, "shenzhen")
    println(student) // com.atguigu.chapter08match.$04_MatchClass$Student@1134affc
    println(student.name)
    println(student.age)
    // println(student.address)  // Cannot resolve symbol address

    println("-------------")
    // 样例类可以直接用于模式匹配
    person match {
      case Person(x,y,z)=>
        println(x)
        println(y)
        println(z)
    }

    println("-------------")
    // 普通类不能直接用于模式匹配,如果想要用于模式匹配需要在伴生对象中定义unapply方法
    val stu = Student("zhagnsan",88,"beijing")
    stu match{
      case Student(x,y,z)=>
        println(x)
        println(y)
        println(z)
    }

  }
}


// 样例类: 其实就是伴生类与伴生对象的封装，但是相比于自己写的，多了方法
// 下面是伴生类和伴生对象
class Student(val name:String, var age:Int, val address:String)

object Student{
  //TODO apply方法是将属性组装成对象
  def apply(name:String,age:Int,address:String) = new Student(name,age,address)

  // 普通类不能直接用于模式匹配,如果想要用于模式匹配需要在伴生对象中定义unapply方法
  // TODO unapply方法是对象拆分为属性值
  def unapply(stu:Student):Option[(String,Int,String)]={
    if(stu==null)
      None
    else
      Some(stu.name, stu.age,stu.address)
  }
}
