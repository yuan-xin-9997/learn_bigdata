package com.atguigu.chapter06objectoriented

object $03_ConstructMethod {

  class Person(val name:String="lisi", var age:Int, address:String, private val sex:String){
    var color:String=_
    def this(color:String) ={
      // 辅助构造方法不能直接构建对象，必须直接或者间接调用主构造方法。
      this(age=100,address="shanghai",sex="women")
      this.color=color
    }

    def getAddress()=this.address
  }

  /**
   * 和Java一样，Scala构造对象也需要调用构造方法，并且可以有任意多个构造方法。
   * Scala类的构造器包括：主构造器和辅助构造器
   *
   * 主构造器中属性使用val/var修饰与不使用val/var修饰的区别：
   *      val/var修饰的非private的属性可以在class内部外部都可以访问
   *      不适用val/var修饰的属性默认其实就是private，因此只能在内部访问
   *
   * 基本语法
   * class 类名(形参列表) {  // 主构造器
       * // 类体
       * def  this(形参列表) {  // 辅助构造器
       * }
       * def  this(形参列表) {  //辅助构造器可以有多个...
       * }
       * }
   * 说明：
   * （1）辅助构造器，函数的名称this，可以有多个，编译器通过参数的个数及类型来区分。
   * （2）辅助构造方法不能直接构建对象，必须直接或者间接调用主构造方法。
   * （3）构造器调用其他另外的构造器，要求被调用构造器必须提前声明。
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val person = new Person(age = 20,sex = "man",address = "shanghai")
    println(person.name)
    println(person.age)
    // println(person.address)
    //println(person.sex)
    person.getAddress
    println("----")
    val person2 = new Person("red")
    println(person2.color)
    println(person2.age)
    println(person2.name)
    println(person2.age)
  }
}
