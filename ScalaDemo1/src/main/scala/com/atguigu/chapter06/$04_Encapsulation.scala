package com.atguigu.chapter06

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter

import scala.beans.BeanProperty

object $04_Encapsulation {

  class Person{
    //private var name:String=_
    //private var age:Int=_
    //def getName()=this.name
    //def getAge()=this.age
    //def setName(name:String)=this.name=name
    //def setAge(age:Int)=this.age=age

    // 等价与上面
    @BeanProperty
    var name: String = _

    @BeanProperty
    var age: Int = _
  }

  /**
   * 属性是类的一个组成部分。
   * 封装就是把抽象出的数据和对数据的操作封装在一起，数据被保护在内部，程序的其它部分只有通过被授权的操作（成员方法），才能对数据进行操作。Java封装操作如下，
   * （1）将属性进行私有化
   * （2）提供一个公共的set方法，用于对属性赋值
   * （3）提供一个公共的get方法，用于获取属性的值
   * Scala中的public属性，底层实际为private，并通过get方法（obj.field()）和set方法（obj.field_=(value)）对
   * 其进行操作。所以Scala并不推荐将属性设为private，再为其设置public的get和set方法的做法。但由于很多Java框架都利
   * 用反射调用getXXX和setXXX方法，有时候为了和这些框架兼容，也会为Scala的属性设置getXXX和setXXX方法（通过@Bean
   * Property注解实现）
   *
   * @BeanProperty 不能用于private修饰的属性上
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val person = new Person
    person.setName("zhangsan")
    person.setAge(11)
    println(person.getAge())

    // 对象转json字符串
    val json = JSON.toJSONString(person, null.asInstanceOf[Array[SerializeFilter]])
    println(json)


    // json转对象
    //val js = "{\"name\":\"xxx\", \"age\":200}"
    var js = """{"name":"xxx", "age":200}"""
    val person2= JSON.parseObject(js, classOf[Person])
    println(person2.getName)

    // obj.field_=(value)  scala自带set方法，不推荐使用
    person2.name_=("yyy")
    println(person2.name)
  }
}
