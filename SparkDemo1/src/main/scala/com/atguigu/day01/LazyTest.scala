package com.atguigu.day01

import scala.io.{BufferedSource, Source}

object LazyTest {
  def main(args: Array[String]): Unit = {
    val datas: Iterator[String] = Source.fromFile("datas/wc.txt").getLines()

    val it2: Iterator[String] = datas.flatMap(line => line.split(" "))

    val it3: Iterator[(String, Int)] = it2.map(x => (x, 1))

//    it3.toList

    // Spark 中Iterator和Iterable区别
    println(it3)

  }
}
