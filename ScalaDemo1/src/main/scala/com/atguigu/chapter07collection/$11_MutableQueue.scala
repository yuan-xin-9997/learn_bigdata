package com.atguigu.chapter07collection
import scala.collection.mutable.Queue
object $11_MutableQueue {
  // 可变队列Queue
  def main(args: Array[String]): Unit = {
      // 队列：先进先出
      // 不可变队列的创建方式：Queue[元素类型](初始元素,...)
      val q1 = Queue[Int](1, 4, 5, 6, 2, 3)
      println(q1)

      // 添加元素
    q1.enqueue(11,22,33)
    println(q1)

    // 删除元素
    q1.dequeue()
    println(q1)

    // 获取元素
    for (elem <- q1) {
      println(elem)
    }

    // 修改元素
    q1(0)=100
    println(q1)

    q1.update(1, 100)
println(q1)

  }
}
