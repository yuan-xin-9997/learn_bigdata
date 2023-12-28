package com.atguigu.chapter07collection

import scala.collection.immutable.Queue

object $10_ImmutableQueue {
  /**
   * 不可变队列Queue
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 队列：先进先出
    // 不可变队列的创建方式：Queue[元素类型](初始元素,...)
    val q1 = Queue[Int](1, 4, 5, 6, 2, 3)
    println(q1)

    // 添加元素
    val q2 = q1.enqueue(10)
    println(q2)

    val q3 = q1.+:(10)
    println(q3)

    val q4 = q1.:+(10)
    println(q4)


    // 删除元素
    println(q1.dequeue)

    // 获取元素
    println(q1)
    println(q1(0))

    // 修改元素
    println(q1.updated(0, 10))

  }
}
