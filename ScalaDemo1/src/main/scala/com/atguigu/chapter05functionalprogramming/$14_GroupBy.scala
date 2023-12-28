package com.atguigu.chapter05functionalprogramming
import java.util

object $14_GroupBy {

  /**
   * 3、按照指定的规则对集合元素进行分组
   * 数据: Array("zhangsan man beijing","lisi woman shanghai","zhaoliu man beijing","hanmeimei woman shenzhen")
   * 规则: 按照性别分组[动态]
   * 结果: Map( man -> List( "zhangsan man beijing", "zhaoliu man beijing" ) , woman -> List( "lisi woman shanghai" ,"hanmeimei woman shenzhen" ) )
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val arr = Array("zhangsan man beijing","lisi woman shanghai","zhaoliu man beijing","hanmeimei woman shenzhen")
    println(groupBy1(arr))
    val func = (x:String)=>x.split(" ")(1)
    println(groupBy2(arr, func))

    println(groupBy2(arr, (x:String)=>x.split(" ")(0)))
    println(groupBy2(arr, (x:String)=>x.split(" ")(2)))
    println(groupBy2(arr, (x)=>x.split(" ")(2)))
    println(groupBy2(arr, x=>x.split(" ")(2)))
    println(groupBy2(arr, _.split(" ")(2)))

  }
   // 分组方法
  def groupBy1(arr:Array[String]) = {
    // 创建Java Map对象
    val resultMap = new util.HashMap[String, util.List[String]]()

    // val elem_arr = for (elem <- arr) {elem.split(" ")}
    for (elem <- arr) {
      val key = elem.split(" ")(1)

      if(resultMap.containsKey(key)){
        val list = resultMap.get(key)
        list.add(elem)
      }else{
        val list = new util.ArrayList[String]()
        list.add(elem)
        resultMap.put(key, list)
      }
    }
    resultMap
  }

  def groupBy2(arr: Array[String], func:String=>String) = {
    // 创建Java Map对象
    val resultMap = new util.HashMap[String, util.List[String]]()

    // val elem_arr = for (elem <- arr) {elem.split(" ")}
    for (elem <- arr) {
      val key = func(elem)

      if (resultMap.containsKey(key)) {
        val list = resultMap.get(key)
        list.add(elem)
      } else {
        val list = new util.ArrayList[String]()
        list.add(elem)
        resultMap.put(key, list)
      }
    }
    resultMap
  }







}
