package com.atguigu.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 序列化原因：spark算子函数体外面的代码是在Driver中执行的，函数体体里面的代码是在Executor Task中执行，
 *            如果函数体中使用了Driver的对象，spark会将该对象序列化后通过网络传输递给Task使用。
 *            如果该Driver中的对象没有继承序列化接口，就不能序列化，Task无法使用户报错
 *
 * spark序列化方式：
 *        Java序列化：比较重，会将类的继承信息、类的属性值、类的属性类型、全类名等全部序列化进去
 *        Kryo序列化：比较轻，只会将类的属性值、类的属性类型、全类名等全部序列化进去
 *        Kryo序列化性能比Java序列化快10倍左右
 *        工作中一般使用Kryo序列化，Spark默认使用Java序列化
 *
 * 如何设置Spark序列化方式
 *       1、在SparkConf中设置new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 *       2、注册哪些类使用Kryo序列化【可选】，.registerKryoClasses(Array(classOf[类名],...))
 */
object $05_Serialization {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        registerKryoClasses(Array(classOf[Person]))
      val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(1,2,3,4,5))
    val x: Int = 10  // Driver
    val rdd2 = rdd.map(y => y * x)  // 闭包函数，用到了外部变量x   // 函数是在Executor Task中执行
    // TODO Driver和Task不在同一个服务器上，x需要序列化，进行网络传输
    println(rdd2.collect().toList)

    val p = new Person

    // todo 报错 Caused by: java.io.NotSerializableException: com.atguigu.day04.Person，
    //  p对象在Driver中定义，并且在task使用
    // val rdd3 = rdd.map(y=>y*p.a)
    //println(rdd3.collect().toList)

    // todo 报错 Caused by: java.io.NotSerializableException: com.atguigu.day04.Person
    //  p对象在Driver中定义，并且在task使用，其中属性a=10是在Driver中定义的，所以报错
    // val rdd4 = p.m1(rdd)
    // println(rdd4.collect().toList)  // 依然报错：

    // todo 不报错，b是局部变量，在Task中定义的，不是Driver中定义的
    val rdd5 = p.m2(rdd)
    println(rdd5.collect().toList)


    // todo 不报错，new Person是在task中定义的，在task中使用，不需要网络传递，不需要序列化
    val rdd6 = rdd.map(x=>x*new Person().a)
    println(rdd6.collect().toList)
  }
}

/**
 * 未实现序列化接口的类
 */
class Person{
  val a = 10
  def m1(rdd: RDD[Int]): RDD[Int] = {
    rdd.map(x => x * a)
  }
  def m2(rdd: RDD[Int]): RDD[Int] = {
    val b = 10
    rdd.map(x => x * b)
  }
}

// 序列化类需要继承序列化接口
class Person1 extends Serializable  {
  val a = 10
  def m1(rdd: RDD[Int]): RDD[Int] = {
    rdd.map(x => x * a)
  }
  def m2(rdd: RDD[Int]): RDD[Int] = {
    val b = 10
    rdd.map(x => x * b)
  }
}