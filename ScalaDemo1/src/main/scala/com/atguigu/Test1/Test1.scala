package com.atguigu.Test1

import scala.io.Source

object Test1 {

  /**
   * 练习题
   * 1、获取没有农贸市场的省份
   * 2、统计菜的种类数最多的三个省份
   * 3、统计每个省份菜的种类数最多的三个农贸市场
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // 1. 读取数据
    val allProvinces = Source.fromFile("datas/allprovince.txt").getLines().toList
    val products = Source.fromFile("datas/product.txt").getLines().toList

    println(allProvinces.size)
    println(allProvinces)
    println(products.size)

    // 1、获取没有农贸市场的省份
    println("-------------")
    test1(products, allProvinces)
    println("-------------")
    // 2、统计菜的种类数最多的三个省份
    test2(products)
    println("-------------")
    // 3, 统计每个省份菜的种类数最多的三个农贸市场
    test3(products)
  }


  /**
   * 1、获取没有农贸市场的省份
   */
  def test1(product:List[String], allProvinces:List[String]): Unit = {
    // 是否需要过滤：需要，需要过滤脏数据
    val filterProduct = product.filter( line=>line.split("\t").size  == 6)
    println(filterProduct.size)

    // 是否需要列裁剪：需要，只需要省份列
    val provinceProducts = filterProduct.map(line => {
      val arr = line.split("\t")
      arr(4)
    })
    println(provinceProducts.size)

    // 是否需要去重：需要
    val distinctProducts = provinceProducts.distinct
    println(distinctProducts.size)

    // 全国所有省份 与 有农贸市场的省份 求差集
    val res = allProvinces.diff(distinctProducts)

    // 打印结果
    res.foreach(println)
  }

  /**
   * 2、统计菜的种类数最多的三个省份
   */
  def test2(product:List[String]): Unit = {

    // 1.是否需要过滤：需要，需要过滤脏数据
    val filterProduct = product.filter(line => line.split("\t").size == 6)
    println(filterProduct.size)

    // 2. 是否需要列裁剪：需要，只需要省份列、菜名
    //List((湖北省，西蓝花).(湖北省，青菜).(湖北省，白菜),,...)
    val provinceProducts = filterProduct.map(line => {
      val arr = line.split("\t")
      val province = arr(4)
      val name = arr.head
      (province, name )
    })
    println(provinceProducts.size)

    // 3. 是否需要去重：需要
    //List((湖北省，西蓝花).(江西省，青菜).(湖南省，白菜),,...)
    val distinctProducts = provinceProducts.distinct
    println(distinctProducts.size)

    // 4. 按照省份进行分组
    // Map(
    //      湖北省->List((),().(),...)
    //      江西省->List((),().(),...)
    // )
//    val groupbyProduct = distinctProducts.groupBy({
//      case (province, name) => province
//    })
    val groupbyProduct = distinctProducts.groupBy({
      x=>x._1
    })
    println(groupbyProduct.size)
    println(groupbyProduct)

    // 5. 统计每个省份的菜的种类数
    // Map(湖北省->23,江西省->22,...)
    val numProduct = groupbyProduct.map(x => {
      (x._1, x._2.size)
    })
    println(numProduct)
    println(numProduct.size)

    // 6. 按菜的种类排序取前三
    val numListProduct = numProduct.toList
    println(numListProduct)
    val top3Product = numListProduct.sortBy(x => x._2).reverse.take(3)
    println(top3Product)

    // 7. 输出结果
    top3Product.foreach(println)

  }

  /**
   * 统计每个省份菜的种类数最多的三个农贸市场
   */
  def test3(product:List[String]): Unit = {
    // 统计每个省份菜的种类数最多的三个农贸市场

    // 1.是否需要过滤：需要，需要过滤脏数据
    val filterProduct = product.filter(line => line.split("\t").size == 6)
    println(filterProduct.size)

    // 2. 列裁剪，(省份、农贸市场名称、菜名)
    // List( (湖北省,A农贸市场,蔬菜),(),(),...)
    val selectProduct = filterProduct.map(x => {
      val arr = x.split("\t")
      (arr(4), arr(3), arr.head)
    })
    println(selectProduct.size)

    // 3.去重[同一个省份、同一农贸市场，同一菜只有一条数据]
    // List( (湖北省,A农贸市场,蔬菜),(),(),...)
    val distinctProduct = selectProduct.distinct
    println(distinctProduct.size)

    // 4. 按照省份+农贸市场分组
    // Map(
    //  (湖北省,A农贸市场)->List(  (湖北省,A农贸市场,蔬菜A),(湖北省,A农贸市场,蔬菜B), )
    //  (江西省,A农贸市场)->List(  (湖北省,A农贸市场,蔬菜A),(湖北省,A农贸市场,蔬菜B), )
    //
    // )
    val groupedProduct = distinctProduct.groupBy({
      case (province, market, fruit) => (province, market)
    })
    println(groupedProduct.head)
    println(groupedProduct.size)

    // 5. 统计每个省份每个农贸市场的菜种类数
    // Map(  (湖北省,A农贸市场)->15, (江西省,A农贸市场)->15,   )
    val groupProductProvince = groupedProduct.map(
      x => (x._1, x._2.size)
    )
    println(groupProductProvince.size)
    println(groupProductProvince.head)

    // 6. 按照省份分组
    // Map(
    //    湖北省->Map (    (湖北省,A农贸市场)->15,(湖北省,B农贸市场)->16    )
    // )
    val groupProvince = groupProductProvince.groupBy(
      { case   ((province, market), num) => province }
    )
    println(groupProvince.size)
    println(groupProvince.take(10))

    // 7. 对每个省份所有农贸市场数据，按照菜的种类排序取前三
    val result = groupProvince.map(x => {
      // x = 湖北省->Map (    (湖北省,A农贸市场)->15,(湖北省,B农贸市场)->16    )
      val top3 = x._2.toList.sortBy(y => y._2).reverse.take(3)

      // 去掉省份列
      // val t3 = top3.map{case ((province, market), num) => (market, num)}
      val t3 = top3.map(
       {case ((province, market), num) => (market, num)}
      )
      // val t3 = top3.map(x=> (x._1._2, x._2))

      // 拼接元组
      (x._1, t3)
    })
    println(result.take(3))
    println(result.size)

    // 8.打印结果
    result.foreach(println)

  }

}
