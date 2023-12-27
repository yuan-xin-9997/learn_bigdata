package com.atguigu.chapter07collection

object $03_ImmutableList {
  def main(args: Array[String]): Unit = {
    // 不可变list的创建方式，List[元素类型](初始元素,....)
    val list = List[Int](1, 5, 4, 3)

    // 添加元素
    // :: 添加单个元素，等价于+:
    val list2 = list.::(30)
    println(list)
    println(list2)

//    list :: 30
    val list3 = 40::30 :: list
    println(list3)

    // Nil相当于空的不可变List，::方法在使用空格调用的时候，在最右边必须是不可变List或者Nil
    val list4 = 40 :: 30 :: Nil
    println(list4)

    // :::添加一个集合中所有元素，等价于++:
    println(List(111, 222, 333) ::: List(555, 666, 777))
    println(List(111, 222, 333).:::(List(555, 666, 777)))


    // 删除元素，不可变集合不可以删除元素

   // 获取角标元素：集合名(角标)
    println(list4(0))

    // 修改元素
//    list4(0) = 10  // value update is not a member of List[Int]
val list5 = list.updated(0, 10)
    println(list5)



  }
}
