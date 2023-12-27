package com.atguigu.chapter07collection

import scala.collection.mutable.ArrayBuffer

object Demo {
  /**
   * （1）Scala的集合有三大类：序列Seq、集Set、映射Map，所有的集合都扩展自Iterable特质。
   * （2）对于几乎所有的集合类，Scala都同时提供了可变和不可变的版本，分别位于以下两个包。
   * 不可变集合：scala.collection.immutable
   * 可变集合：  scala.collection.mutable
   * （3）Scala不可变集合，就是指该集合对象不可修改，每次修改就会返回一个新对象，而不会对原对象进行修改。类似于java中的String对象。
   * （4）Scala可变集合，就是这个集合可以直接对原对象进行修改，而不会返回新的对象。类似于java中StringBuilder对象。
   * 建议：在操作集合的时候，不可变用符号，可变用方法。
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // TODO 易混淆点
    // val var
    // ArrayBuffer Array
    // 可变         不可变
    // 不定长/变长   定长
    val arr1 = ArrayBuffer
    var arr2 = Array

    // 可变集合：集合的长度可变，意味着可以在原集合中添加/删除元素
    // 不可变集合：集合的长度不可变，意味着不可以在原集合中添加/删除元素
  }
}
