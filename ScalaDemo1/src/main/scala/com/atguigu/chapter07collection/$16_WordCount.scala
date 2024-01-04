package com.atguigu.chapter07collection

import scala.io.Source

object $16_WordCount {
  /**
   * WordCount案例
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 1. 读取数据
    val list: List[String] = Source.fromFile("datas/wc.txt").getLines().toList
    """
      |List("hello java spark hadoop",
            |"hadoop flume kafka scala",
            |"scala java hello hello",
            |"hello scala java flume",
            |"flume hadoop spark kafka)
      |""".stripMargin
    println(list)

    // 2. 切割 + 炸开
    // List(hello, java, spark, hadoop...)
    val list2: List[String] = list.flatMap(line => line.split(" "))
    println(list2)

    // 3. 按照单词分组
    """
      |Map(
      |     hello->List(hello, hello, ...)
      |     java->List(java, java, ...)
      |     hadoop->List(hadoop, hadoop, ...)
      |)
      |""".stripMargin
    val list3: Map[String, List[String]] = list2.groupBy(x => x)
    println(list3)

    // 4. 统计每个组中单词的个数
    // Map(
    //    "hello"->4,
    //    "java"->3,
    // )
    val list4: Map[String, Int] = list3.map(x => {
      (x._1, x._2.size)
    })
    println(list4)

    // 5. 输出结果
    list4.foreach(x=> println(x) )


    // todo  以上为分步骤执行，以下可以采用链式编程的方式，一步输出
    println("---------------------------------------")
    Source.fromFile("datas/wc.txt").getLines()
      .toList
      .flatMap(_.split(" "))
      .groupBy(x=>x)
      .map(x=>(x._1, x._2.size))
      .foreach(println)




  }
}
