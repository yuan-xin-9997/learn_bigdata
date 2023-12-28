package com.atguigu.chapter06objectoriented

object $02_FieldsAndMethod {

  class Person{
    val name="lisi"
    var aget = 20
    private val sex="man"
    var f1:Boolean=_
    var f2:Int=_
    var f3:Double=_

    private def m1(x:Int, y:Int)=x*y
    def m2(x:Int, y:Int)=x*y

  }

  /**
   * java 定义属性：[访问修饰符] 类型 属性名 [=值]
   *
   *
   * scala 定义属性：[访问修饰符] val/var 属性名:类型 =值
   * scala的访问修饰符
   *      protect
   *      private
   *      public(默认)
   * scala中对于var修饰的属性可以使用_赋予初始值，使用_赋予初始值的时候，必须指定属性的类型（本地/局部变量_不可以使用）
   * scala 定义方法 [访问修饰符] def 方法名(参数名:类型,...):返回值类型={方法体}
   *
   * 在Java中，访问权限分为：public， protected，默认,和private 。在Scala中，你可以通过类似的修饰符达到同样的效果。但是使用上有区别。
   * （1）Scala 中属性和方法的默认访问权限为public，但Scala中无public关键字。
   * （2）private为私有权限，只在类的内部和伴生对象中可用。
   * （3）protected为受保护权限，Scala中受保护权限比Java中更严格，同类、子类可以访问，同包无法访问。
   * （4）private[包名]增加包访问权限，包名下的其他类也可以使用
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val person = new Person
    println(person.name)
    println(person.aget)
    //println(person.sex)
    person.aget=22
    println(person.aget)
    println(person.f1)
    println(person.f2)
    println(person.f3)
//    person.m1(1,2)
    println(person.m2(1, 2))
  }

}
