package com.atguigu.chapter07collection

object $08_ImmutableMap {
  /**
   * 不可变Map
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 不可变Map的创建：Map[K的类型, V的类型](K->V, (K,V), ... )
    val map = Map[String, Int]("aa"->10, ("bb", 20), "aa"->100, "cc"->22)
    println(map)

    // 添加元素
    val map2 = map.+("ee"->30)
    val map3 = map.++(List("ff" -> 11, "pp" -> 32, "oo" -> 34))
    val map4 = map.++:(List("ff" -> 11, "pp" -> 32, "oo" -> 34))
    println(map)
    println(map2)
    println(map3)
    println(map4)

    // 删除元素
    val map5 = map4.-("oo")
    val map6 = map4.--(List("pp", "aa", "ff"))
    println(map5)
    println(map6)

    // 获取元素
    println(map4.get("bb"))
    println(map4.get("bb").get)
    println(map4.get("bb1"))
    // todo Option为了提示外部当前结果可能为空，需要处理
    //      Option有两个子类，Some，None
    //      Some 代表结果不为空，结果封装在Some中
    //      None 代表结果为空
    // 一般用getOrElse获取值
    println(map4.getOrElse("bb", 0))
    println(map4.getOrElse("bb1", 0))

    // 修改map key的value的值
    println(map4)
    val map7 = map4.updated("bb", 400)
    println(map7)
    println(map4)

    println(map4.updated("bb1", 40))
  }
}
