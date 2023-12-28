package com.atguigu.chapter07collection

object $14_CollectionLowerFunction {

  /**
   * 集合初级计算函数
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val list = List(1,2,3,4,6,5)
    // 最大值，针对可排序数据类型元素
    println(list.max)
    println(list.maxBy(x => x))

    // 最大值maxBy: 根据func函数返回值取最大元素，针对所有数据类型元素
    val list2 = List(("lisi", 100), ("wangwu", 200), ("hanmeimei", 1000))
    val func = (x:(String,Int))=>x._2
    println(func)
    println(list2)
    println(list2.maxBy(func))
    println(list2.maxBy((x:(String,Int))=>x._2))
    println(list2.maxBy(x => x._2))
    println(list2.maxBy(_._2))

    // 最小值
    println(list.min)

    // 求和
    println(list.sum)

    // 排序
    // sorted 直接按照元素本身排序，默认升序
    println(list.sorted)
    println(list.sorted.reverse)

    println(list2.sorted)
    // sortBy(func:集合元素类型=>B) = SQL sorted by  指定按照哪个字段排序   默认升序
    println(list2.sortBy(x => x._2))
    println(list2.sortBy(x => -x._2))
    println(list.sortBy(x => x))
    println(list.sortBy(x => -x))

    // sortWith(func:(集合元素类型, 集合元素类型)=>Boolean)  按照排序规则排序
    println(list.sortWith((x, y) => if (x < y) true else false))
    println(list.sortWith((x, y) =>x<y))
    println(list.sortWith((x, y) =>x>y))
    println(list2.sortWith((x, y) => x._2 < y._2))
    println(list2.sortWith((x, y) => x._2 > y._2))


    // scala初级、高级函数 完全能替代SQL的基本查询
    """
      |SQL         Spark
      |select      map
      |from t
      |where       filter
      |group by    groupBy
      |order by    sortBy
      |explode     flatMap
      |""".stripMargin

  }
}
