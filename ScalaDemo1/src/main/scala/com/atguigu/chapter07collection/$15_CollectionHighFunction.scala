package com.atguigu.chapter07collection

object $15_CollectionHighFunction {

  /**
   * scala 高阶函数
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // todo map(func:集合元素类型=>B):集合 一对一映射，原集合元素经过计算之后得到新集合一个元素
   //       map函数针对每个元素操作，元素个数=函数执行次数
    //      新集合元素个数=原集合元素个数
    //      使用场景：用于数据类型/值的转换（一堆一转换
    //      类似于SQL 的select
    val list = List(1,2,3,4,5)
    val func:Int=>Int = x=>x*10
    val list1 = for (elem <- list)  yield {func(elem)}
    println(list1)
    println(list.map(func))  // 和上面有yield关键字的for循环类似

    // todo foreach(func:集合元素类型=>B):Unit     对每个元素进行遍历
    //          函数针对每个元素操作，元素个数=函数执行次数
    //  foreach与map区别
    //          foreach相当于没有yield关键的for循环，foreach不会产生新集合
    //          map相当于有yield关键的for循环，map会产生新集合
    list.foreach(x=>println(x))
    list.foreach(println(_))
    list.foreach(println)

    // todo flatten 压平  相当于SQL的explode(数组)炸开
    //     针对的是集合嵌套集合的数据类型
    //     仅将第二层集合压掉，保留元素
    //     生成新集合的元素个数一般式>=原集合元素个数
    val list3 = List( List(1,2,3),  List(4,5,6, List(7,8,9))   )
    println(list3.flatten)
    // println(list3.flatten.flatten)
    //println(list.flatten)
    println(list3.flatMap(x => x))

    // todo flatMap(func:集合元素类型=>集合) = 先map(func:集合元素类型=>集合) + 再flatten   = 数据转换+压平
    //       flatMap里面函数执行次数=元素个数
    //       flatMap先执行map操作，然后对map返回集合元素进行压平
    //       flatMap应用场景：一对多转换
    val list7 = List(
      "hello java spark",
      "hello java hadoop",
      "hello java flink",
    )
    // 统计单词个数
    val list8 = list7.map(x=>x.split(" "))
    println(list8)
    val list9 = list8.flatten
    println(list9)

    println(list7.flatMap(x => x.split(" ")))

    // 不可以将func写到外面：found   : String => Array[String]
    //                              required: String => scala.collection.GenTraversableOnce[?]
    val func1 = (x:String)=>x.split(" ")
    // println(list7.flatMap(func1))

    // todo filter(func:集合元素类型=>Boolean) 按照指定条件过滤
    //       里面函数执行次数=元素个数
    //       保留的是函数返回值为true的数据
    //       类似与SQL中的where
    //       场景：用于过滤脏数据或者其他不符合要求的数据
    val list100 = List(10,2,1,6,8)
    println(list100.filter(x => x % 2 == 0))
    println(list100.filter(_ % 2 == 0))



    // todo groupBy(func:集合元素类型=>K)  按照指定字段分组
    //     里面的函数func针对的是每个元素操作
    //     按照func函数返回值分组，值相同的在一组
    //     生成的Map集合，K是函数的返回值，V是K对应原集合中所有元素的结合
    val list10 = List( ("lisi","man","shenzhen"),("wangwu","woman","beijing"),("zhaoliu","man","beijing"),("qianqi","woman","guangzhou") )
    println(list10.groupBy(x => x._3))
    val list11 = list10.groupBy(x => x._3)  // 实现分组功能，但是没有统计个数
    val list12 = list11.map(
      x=>{
        (x._1, x._2.size)
      }
    )
    println(list12)


    // 聚合函数
    // todo reduce(func(集合元素类型,集合元素类型)=>集合元素类型)  从左向右对集合所有元素聚合
    //      reduce函数第一个参数代表上一次的聚合结果，第一次聚合的时候，参数初始值=集合第一个元素
    //      reduce函数第二个参数代表当前待聚合的函数
    val list13 = List(10,30,20,50,60)
    println(list13.reduce((tmp, cur) => {
      println(s"tmp=${tmp} curr=${cur}")
      tmp + cur
    }))

    // todo reduceRight
    //  reduceRight(func(集合元素类型,集合元素类型)=>集合元素类型)  从右向左对集合所有元素聚合
    //      reduceRight函数第二个参数代表上一次的聚合结果，第一次聚合的时候，参数初始值=集合第一个元素
    //      reduceRight函数第一个参数代表当前待聚合的函数
    println(list13.reduceRight((tmp, cur) => {
      println(s"tmp=${tmp} curr=${cur}")
      tmp + cur
    }))

    // todo fold(默认值)(func(集合元素类型,集合元素类型)=>集合元素类型)  从左向右对集合所有元素聚合
    //         fold函数第一个参数代表上一次的聚合结果，第一次聚合的时候，参数初始值=默认值
    //         fold函数第二个参数代表当前待聚合的函数
    println(list13.fold(0)((tmp, cur) => {
      println(s"tmp=${tmp} curr=${cur}")
      tmp + cur
    }))

    // todo foldRight
    //   foldRight(默认值)(func(集合元素类型,集合元素类型)=>集合元素类型)  从右向左对集合所有元素聚合
    //        foldRight函数第二个参数代表上一次的聚合结果，第一次聚合的时候，参数初始值=默认值
    //        foldRight函数第一个参数代表当前待聚合的函数
    println(list13.foldRight(0)((tmp, cur) => {
      println(s"tmp=${tmp} curr=${cur}")
      tmp + cur
    }))
  }
}
