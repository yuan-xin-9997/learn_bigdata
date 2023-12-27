package com.atguigu.chapter06

object $05_Object {



  /**
   * java中存在静态属性、静态方法、非静态属性、非静态方法。
   * scala当中不存在静态与非静态。object中定义的所有属性与方法、函数,除开private修饰的，
   * 都可以通过对象名.属性、对象名.方法、对象名.函数 的方式调用，可以理解为java中的static修饰的。
   *
   *
   * object 修饰的就是单例对象
   * 获取object单例的对象：object名称
   * scala object中所有属性和方法都是类似java static修饰的，可以通过object名称.成员名的方式调用
   * scala class中所有的属性和方法都是类似java非static修饰的，可以通过对象.成员名的方式调用
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // object 修饰的就是单例对象
    //   * 获取object单例的对象：object名称
    println(PersonDemo1)
    println(PersonDemo1)
    println(PersonDemo1)
    println(PersonDemo1)
    // object中所有属性和方法都是类似java static修饰的，可以通过object名称.成员名的方式调用
    println(PersonDemo1.name)
    // println(PersonDemo1.sex)  // Symbol sex is inaccessible from this place



    // class中所有的属性和方法都是类似java非static修饰的，可以通过对象.成员名的方式调用
    val p1 = new PersonDemo2(age=11, address = "beijing", sex = "male")
    println(p1.name)
//    println(PersonDemo2.name) // 报错
  }
}

// 单例对象
object PersonDemo1{
  val name = "lisi"
  private val sex = "m"
}

class PersonDemo2(val name: String = "lisi444", var age: Int, address: String, private val sex: String) {
  var color: String = _

  def this(color: String) = {
    // 辅助构造方法不能直接构建对象，必须直接或者间接调用主构造方法。
    this(age = 100, address = "shanghai", sex = "women")
    this.color = color
  }

  def getAddress() = this.address
}