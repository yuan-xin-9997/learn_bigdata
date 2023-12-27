package com.atguigu.tutorial



import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.mutable.Set
import scala.collection.mutable.Map

/**
 * 来源：https://mp.weixin.qq.com/s/E_Y2qZkNiDY1uE1I3uN47Q
 */
object Tutorial {

  def main(args: Array[String]): Unit = {
    array_example()
    tuple_example()
    list_example()
    set_example()
    map_example()
    iterator_example()
    function_programming_example()
  }

  // 数组：scala中数组的概念是和Java类似。数组里面元素类型需要一致
  // 可以用数组来存放一组数据。scala中，有两种数组，一种是定长数组，另一种是变长数组
  def array_example(): Unit = {
    // 定长数组
    //    定长数组指的是数组的长度是不允许改变的
    //    数组的元素是可以改变的
    //    java中数组是根据类型来定义的比如 int[]  String[]
    //    在Scala中, 数组也是一个类, Array类, 存放的内容通过泛型来定义, 类似java中List的定义
    // 通过指定长度定义数组
    //   val/var 变量名 = new Array[元素类型](数组长度)
    // 用元素直接初始化数组
    //   val/var 变量名 = Array(元素1, 元素2, 元素3...)
    // [!NOTE]
    //
    //    在scala中，数组的泛型使用[]来指定(java <>)
    //    使用()来获取元素(java [])

    // 示例：定义一个长度为100的整型数组
    //设置第1个元素为110
    //打印第1个元素
    val a = new Array[Int](100)
    //a(0) = 110  // todo 定长Array长度不可以变，但是元素可以变
    println(a)
    //println(a(0))
    // a += "0"

    // 示例;定义一个包含以下元素的数组 :"java", "scala", "python"
    //获取数组长度
    val b = Array("java", "scala", "python")
    println(b.length)

    // 为什么带初值的就不用new呢.
    //这里会用到一个apply方法, 我们后面会详细说.
    //我们现在只需要知道, 直接Array(1, 2, 3)来创建数组, 其实就是自动调用了Array类中的apply方法
    //apply方法做的事情就是, new array(3)  然后把3个元素放进去, 也就是这些工作自动帮我们做了.
    // 大家先记住结论, 先会用, 后面我们学到面向对象的时候就会明白为什么啦


    // 变长数组
    //变长数组指的是数组的长度是可变的，可以往数组中添加、删除元素
    // 创建变长数组，需要提前导入ArrayBuffer类import scala.collection.mutable.ArrayBuffer
    //语法
    //创建空的ArrayBuffer变长数组，语法结构：
    //  val/var a = ArrayBuffer[元素类型]()
    //创建带有初始元素的ArrayBuffer
    //  val/var a = ArrayBuffer(元素1，元素2，元素3....)

    // 示例一
    //定义一个长度为0的整型变长数组
    val c = ArrayBuffer[Int]()
    println(c)

    // 示例二：示例二
    //定义一个包含以下元素的变长数组
    //"hadoop", "storm", "spark"
    val d = ArrayBuffer("hadoop", "storm", "spark")
    println(d)

    // 添加 / 修改 / 删除元素
    // 使用+=添加元素
    //使用-=删除元素
    //使用++=追加一个数组到变长数组

    // 示例
    //定义一个变长数组，包含以下元素: "hadoop", "spark", "flink"
    //往该变长数组添加一个"flume"元素
    //从该变长数组删除"hadoop"元素
    //再将一个数组，该数组包含"hive", "sqoop"追加到变长数组中
    // 定义变长数组

    val e = ArrayBuffer("hadoop", "spark", "flink")

    // 追加一个元素
     e += "flume"
    // e += 1  // 需要相同类型
    println(e)

    // 删除一个元素
    e -= "hadoop"
    println(e)

    // 追加一个数组
    e ++= Array("hive", "sqoop")
    e(0) = "jhjjjjj"   // 改变数组元素
    println(e)

    // 遍历数组
    // 可以使用以下两种方式来遍历数组：
    //
    //使用for表达式直接遍历数组中的元素
    //
    //使用索引遍历数组中的元素

    // 示例一
    //
    //定义一个数组，包含以下元素1,2,3,4,5
    //使用for表达式直接遍历，并打印数组的元素
    val f = Array(1,2,3,4,5)
    for (elem <- f) {
      println(elem)
    }
    println("-"*100)

    // 示例二
    //
    //定义一个数组，包含以下元素1,2,3,4,5
    //使用for表达式基于索引下标遍历，并打印数组的元素
    val g = Array(1,2,3,4,5)
    for (i<- 0 to g.length-1  ) {println(g(i))}
    for(i <- 0 to g.length - 1) println(g(i))

    // [!NOTE]
    //
    //0 until n——生成一系列的数字，包含0，不包含n
    //
    //0 to n ——包含0，也包含n

    // 10.3 数组常用算法
    //scala中的数组封装了一些常用的计算操作，将来在对数据处理的时候，不需要我们自己再重新实现。以下为常用的几个算法：
    //
    //求和——sum方法
    //求最大值——max方法
    //求最小值——min方法
    //排序——sorted方法

    // 求和
    //数组中的sum方法可以将所有的元素进行累加，然后得到结果

    // 例
    //
    //定义一个数组，包含以下几个元素（1,2,3,4)
    //请计算该数组的和
    val a1 = Array(1,2,3,4)
    println(a1.sum)
    // 最大值
    //数组中的max方法，可以获取到数组中的最大的那个元素值
    println(a1.max)
    // 最小值
    //数组的min方法，可以获取到数组中最小的那个元素值
    println(a1.min)
    // 排序
    //数组的sorted方法，可以对数组进行升序排序。而reverse方法，可以将数组进行反转，从而实现降序排序
    println(a1.sorted)
    for (elem <- a1.sorted) {println(elem)}
    for (elem <- a1.sorted.reverse) {
      println(elem)
    }
  }

  // 元组可以用来包含一组不同类型的值。例如：姓名，年龄，性别，出生年月。元组的元素是不可变的。
  // 数组: 同一类数据成组
  //
  //元组: 不同的元素成组
  //
  //元祖内容(元素)不可变
  def tuple_example() = {
    println("=========元组==========")
    // 定义元组
    //语法
    //
    //方式1: 使用括号来定义元组
    //
    //val/var 元组 = (元素1, 元素2, 元素3....)
    //方式2: 使用箭头来定义元组（元组只能有两个元素）
    //
    //val/var 元组 = 元素1->元素2
    //这里预告一下, 使用箭头创建元组, 数量被限制到了2, 这个是有原因的.
    //
    //一般两个元素是key value格式比较多, 后面我们就能感受到了

    // 分别使用括号、和箭头来定义元组
  val a = (1, "zhangsan", 20, "beijing")
    println(a)
     val b = "zhangsan" -> 20
    println(b)

    // 访问元组
    //使用_1、_2、_3....来访问元组中的元素，_1表示访问第一个元素，依次类推
    println(a._1)
    println(a._2)

  }

  // 列表是scala中最重要的、也是最常用的数据结构。List具备以下性质：
  //
  //可以保存重复的值
  //有先后顺序
  //在scala中，也有两种列表，一种是不可变列表、另一种是可变列表
  def list_example(): Unit = {
    println("=======列表=========")

    // 定义
    //不可变列表就是列表的元素、长度都是不可变的。
    //
    //语法
    //
    //使用List(元素1, 元素2, 元素3, ...)来创建一个不可变列表，语法格式：
    //
    //val/var 变量名 = List(元素1, 元素2, 元素3...)
    //使用Nil创建一个不可变的空列表
    //
    //val/var 变量名 = Nil
    //使用::方法创建一个不可变列表
    // val/var 变量名 = 元素1 :: 元素2 :: Nil
    //[!TIP]
    //
    //使用::拼接方式来创建列表，必须在最后添加一个Nil

    // 示例一
    // 创建一个不可变列表，存放以下几个元素（1,2,3,4）
    val a = List(1,2,3,4)
    println(a)
    println(a(0))

    // 示例二
    //使用Nil创建一个不可变的空列表
    //
    //参考代码
    //
    val b = Nil
    println(b)
//    示例二
//    使用Nil创建一个不可变的空列表
    val c = -2 :: -1 :: Nil
    println(c)

    // 11.1 可变列表
    //可变列表就是列表的元素、长度都是可变的。
    //
    //要使用可变列表，先要导入import scala.collection.mutable.ListBuffer
    //
    //[!NOTE]
    //
    //可变集合都在mutable包中
    //不可变集合都在immutable包中（默认导入）
    //定义
    //使用ListBuffer[元素类型]()创建空的可变列表，语法结构：
    //
    //val/var 变量名 = ListBuffer[Int]()
    //使用ListBuffer(元素1, 元素2, 元素3...)创建可变列表，语法结构：
    //
    //val/var 变量名 = ListBuffer(元素1，元素2，元素3...)

    // 示例一
    //创建空的整形可变列表
    //
    //参考代码
    //
    val a1 = ListBuffer[Int]()
    println(a1)

    // 示例二
    //创建一个可变列表，包含以下元素：1,2,3,4
    //
    //参考代码
    //
    val a2 = ListBuffer(1,2,3,4)
    println(a2)

    """
      |可变列表操作
      |获取元素（使用括号访问(索引值)）
      |添加元素（+=）
      |追加一个列表（++=）
      |更改元素（使用括号获取元素，然后进行赋值）
      |删除元素（-=）
      |转换为List（toList）转换为不可变列表
      |转换为Array（toArray）转换为定长数组
      |说一下,
      |
      |定长数组Array 长度固定, 元素可变
      |
      |不可变List, 长度固定, 元素不可变
      |
      |大家不要记混淆了, 怎么记呢, 看名字
      |
      |定长数组, 就是只是固定长度, 元素可变
      |
      |不可变List, 不可变了就是元素和长度都不可以变
      |""".stripMargin

//    示例
//    定义一个可变列表包含以下元素
//    ：1
//    , 2
//    , 3
//    获取第一个元素
//    添加一个新的元素
//    ：4
//    追加一个列表
//    ，该列表包含以下元素
//    ：5
//    , 6
//    , 7
//    删除元素7
//    将可变列表转换为不可变列表
//    将可变列表转换为数组

    // 创建可变列表
    val a3 = ListBuffer(1, 2, 3)

    // 获取第一个元素
    println(a3(0))

    // 追加一个元素
    a3 += 5
    println(a3)

    // 追加一个列表
    a3 ++= List(5,6,7)
    println(a3)

    // 删除元素
    a3 -= 7
    println(a3)

    // 转换为不可变列表
    println(a3.toList)

    // 转换为数组
    println(a3.toArray)

    """
      |
      |11.2 列表常用操作
      |以下是列表常用的操作
      |
      |判断列表是否为空（isEmpty）
      |拼接两个列表（++）
      |获取列表的首个元素（head）和剩余部分(tail)
      |反转列表（reverse）
      |获取前缀（take）、获取后缀（drop）
      |扁平化（flaten）
      |拉链（zip）和拉开（unzip）
      |转换字符串（toString）
      |生成字符串（mkString）
      |并集（union）
      |交集（intersect）
      |差集（diff）
      |""".stripMargin
      // 拼接两个列表
      //示例
      //
      //有两个列表，分别包含以下元素1,2,3和4,5,6
      //使用++将两个列表拼接起来
      //和 ++= 不同的是, ++= 是 追加, 也就是 将一个追加到另一个
      //
      //++ 是两个串联在一起 形成一个新的, 这个概念不要混淆
      //
      //如 a ++= b,  最终是a 变化了 加长了
      //
      //a ++ b 执行完后, a b 均不变, 但结果是a和b的串联, 需要用变量接收
    val a4 = List(1,2,3)
    val b4 = List(4,5,6)
    println(a4 ++ b4)

    // 获取列表的首个元素和剩余部分
    //示例
    //
    //定义一个列表，包含以下几个元素：1,2,3
    //使用head方法，获取列表的首个元素(返回值是单个元素)
    //使用tail方法，获取除第一个元素以外的元素，它也是一个列表(返回值是剩余元素列表)
    val a5 = List(1,2,3)
    // todo 哪怕这里用var 接受一个不可变列表，也不能对列表进行改变
    var a6 = List(4,5,6)
    // a6 += 5 // 哪怕这里用var 接受一个不可变列表，也不能对列表进行改变
    // val定义的是不可重新赋值的变量
    //var定义的是可重新赋值的变量
    // a5 = List(2,3,4) // 报错，eassignment to val
    a6 = List(2,3,4) // 可以对该变量进行重新赋值
    println(a5.head)
    println(a5.tail)


   // 获取列表前缀和后缀
    //示例
    //
    //定义一个列表，包含以下元素：1,2,3,4,5
    //使用take方法获取前缀（前三个元素）：1,2, 3(返回的也是列表)
    //使用drop方法获取后缀（除前三个以外的元素）：4,5(返回的也是列表)
    //是不是和head 和tail 很像?
    //
    //我们可以认为head 就是take(1)然后取出值(因为head返回是单个元素, take返回list)
    //
    //tail 就是 drop(4)
    val a7 = List(1,2,3,4,5)
    println(a7.take(1))
    println(a7.take(3))
    println(a7.take(5))
    println(a7.drop(1))
    println(a7.drop(3))
    println(a7.drop(4))

    // 反转列表
    //示例
    //
    //定一个列表，包含以下元素：1,2,3
    //使用reverse方法将列表的元素反转
    //注意, 列表本身不会变, 只是生成了一个新结果, 需要被变量接收
//    val a8 = List(1,2,3)
//    println(a8.reverse)


    // 扁平化(压平)
    //扁平化表示将列表中的列表中的所有元素放到一个列表中。
    // 示例
    //
    //有一个列表，列表中又包含三个列表，分别为：List(1,2)、List(3)、List(4,5)
    //使用flatten将这个列表转换为List(1,2,3,4,5)
    val a9 = List(List(1,2), List(3), List(4,5))
    println(a9)
    println(a9.flatten)
    // 以使用flatten要注意, 数据要规范才可用
    //
    //也就是说, 目前的flatten方法 适合规范数据, 如果是示例中这样的数据 ,可能需要大家自行实现自己的myFlatten方法啦. 相信大家可以做到的.(毕业后工作了再试, 现在还是抓紧时间学习课堂内容)


    // 拉链与拉开
    //拉链：使用zip将两个列表，组合成一个元素为元组的列表
    //拉开：将一个包含元组的列表，解开成包含两个列表的元组


    //示例
    //
    //有两个列表
    //第一个列表保存三个学生的姓名，分别为：zhangsan、lisi、wangwu
    //第二个列表保存三个学生的年龄，分别为：19, 20, 21
    //使用zip操作将两个列表的数据"拉"在一起，形成 zhangsan->19, lisi ->20, wangwu->21
    val a10 = List("zhangsan", "lisi", "wangwu")
    val b10 = List(19, 20, 21)
    println(a10.zip(b10))

    // 示例
    //
    //将上述包含学生姓名、年龄的元组列表，解开成两个列表
    val res10 = a10.zip(b10)
    println(res10.unzip)


    // 转换字符串
    //toString方法可以返回List中的所有元素
    //示例
    //
    //定义一个列表，包含以下元素：1,2,3,4
    //使用toString输出该列表的元素
    val a11 = List(1,2,3,4,11)
    println(a11.toString)

    // 生成字符串
    //mkString方法，可以将元素以分隔符拼接起来。默认没有分隔符
    //示例
    //
    //定义一个列表，包含以下元素1,2,3,4
    //使用mkString，用冒号将元素都拼接起来
    val a12 = List(1,2,3,12)
    println(a12.mkString)
    println(a12.mkString(","))


    // 并集
    //union表示对两个列表取并集，不去重
    //
    //定义第一个列表，包含以下元素：1,2,3,4
    //定义第二个列表，包含以下元素：3,4,5,6
    //使用union操作，获取这两个列表的并集(类似拼接)
    //使用distinct操作，去除重复的元素(list的方法, 去重)
    val a13 = List(1,2,3,4)
    val b13 = List(3,4,5,6)
    println(a13.union(b13))

    // 交集
    //intersect表示对两个列表取交集
    //取出两个列表中一样的元素
    println(a13.intersect(b13))

    // 差集
    //diff表示对两个列表取差集，例如：a1.diff(a2)，表示获取a1在a2中不存在的元素
    // 同理 a2.diff(a1) 就是取 a2 在 a1中不存在的元素. 不要混淆.
    println(a13.diff(b13))
  }

  /**
   * Set(集)是代表没有重复元素的集合。Set具备以下性质：
   *
   * 元素不重复
   * 不保证插入顺序
   * 和List正好相反, List:
   *
   * 元素可以重复
   * 保证插入顺序
   * scala中的集也分为两种，一种是不可变集，另一种是可变集。
   */
  def set_example(): Unit = {
    println("===========set============")

    // 不可变集 set
    //定义
    //语法
    //
    //创建一个空的不可变集，语法格式：
    //
    //val/var 变量名 = Set[类型]()
    //给定元素来创建一个不可变集，语法格式：
    //
    //val/var 变量名 = Set(元素1, 元素2, 元素3...)

    //示例一
    //定义一个空的不可变集
    //参考代码
    val a = Set[Int]()
    println(a);

    // 示例二
    //定义一个不可变集，保存以下元素：1,1,3,2,4,8
    //
    //参考代码
    //
    //scala>
    val a1 = Set(1,1,3,2,5,9)
    println(a1)
    //// 可以看到 1. 去重了 2. 顺序乱了   这些就是Set的特性

    // 基本操作
    //获取集的大小（size）
    //遍历集（和遍历数组一致）
    //添加一个元素，生成一个Set（+）
    //拼接两个集，生成一个Set（++）
    //拼接集和列表，生成一个Set（++）

    // 示例
    //创建一个集，包含以下元素：1,1,2,3,4,5
    //获取集的大小
    //遍历集，打印每个元素
    //删除元素1，生成新的集
    //拼接另一个集（6, 7, 8)
    //拼接一个列表(6,7,8, 9)
    //参考代码
    // 创建集
    //scala> val
    val a2 = Set(1,1,2,3,4,5)
    // 获取集的大小
    println(a2.size)
    // 遍历集
    for(i <- a2) {
      println(i)
    }

    // 删除一个元素
    println(a2)
    println(a2 - 1)

    // 拼接两个集
    println(a2 ++ Set(6, 7, 8))

    // 拼接集和列表
    println(a2 ++ List(6, 7, 8, 9))

    // 注意, 每次对a的操作, 都是生成了一个新的Set, a自身是不会变化的. 不仅仅指a是不可变集, 同时a 也是val定义的
    // 如果是var 定义的
    var a3 = Set(1, 2, 3, 4, 5)

    println(a3)
    a3 = a3 + 6
    println(a3)

    //// 实际上a 虽然+ 1了 ,但是操作前后的两个a 不是同一个对象.
    //// Set是不可变的, 如果+1 就是生成了新的Set ,同时因为a是var定义的, 所以就可以重新将这个新生成的结果

    // 十三、可变集
    //定义
    //可变集合不可变集的创建方式一致，只不过需要提前导入一个可变集类。
    //
    //手动导入：import scala.collection.mutable.Set

    // 示例
    //定义一个可变集，包含以下元素: 1,2,3, 4
    //添加元素5到可变集中
    //从可变集中移除元素1

    val a4 = Set(1, 2, 3, 4)
    println(a4)

    // 添加元素
     a4 += 5
    println(a4)

    // 删除元素
    a4 -= 1
    println(a4)
  }

  /**
   * 映射：Map可以称之为映射。它是由键值对组成的集合。在scala中，Map也分为不可变Map和可变Map。
   */
  def map_example(): Unit = {
    println("============map=============")

    // 不可变Map
    //定义
    //语法
    //val/var map = Map(键->值, 键->值, 键->值...) // 推荐，可读性更好
    //val/var map = Map((键, 值), (键, 值), (键, 值), (键, 值)...)

    //示例
    //定义一个映射，包含以下学生姓名和年龄数据
    //"zhangsan", 30
    //"lisi", 40
    //获取zhangsan的年龄

    //参考代码
     val map = Map("zhangsan"->30, "lisi"->40)
    println(map)

    //// 根据key获取value
    println(map("zhangsan"))


    // 可变Map
    //定义
    //定义语法与不可变Map一致。但定义可变Map需要手动导入import scala.collection.mutable.Map

    // 示例
    //定义一个映射，包含以下学生姓名和年龄数据
    //
    //"zhangsan", 30
    //"lisi", 40
    //修改zhangsan的年龄为20
    //
    val map1 = Map("zhangsan"->30, "lisi"->40)
    println(map1)

    //// 修改value
    map1("zhangsan") = 20
    println(map1)

    // Map基本操作
    //获取值(map(key))
    //获取所有key（map.keys）
    //获取所有value（map.values）
    //遍历map集合
    //getOrElse
    //增加key,value对
    //删除key

    // 示例
    //定义一个映射，包含以下学生姓名和年龄数据
    //
    //"zhangsan", 30
    //"lisi", 40
    //获取zhangsan的年龄
    //获取所有的学生姓名
    //获取所有的学生年龄
    //打印所有的学生姓名和年龄
    //获取wangwu的年龄，如果wangwu不存在，则返回-1
    //新增一个学生：wangwu, 35
    //将lisi从可变映射中移除
    val map2 = Map("zhangsan"->30, "lisi"->40)
    println(map)
    println(map2("zhangsan"))
    // println(map2("zhangsan1")) // key not found: zhangsan1
    println(map2.keys)
    println(map2.values)
    for (elem <- map2) {
      println(elem)
    }
    for((x,y) <- map2) println(s"$x $y")
    println(map2.getOrElse("wangwu", -1))
    map2 += ("wangwu"->35)
    println(map2)
    map2 + (("ppp", 10), ("iii", 9)) // 或者这样  map + ("ppp" ->10, "iii" -> 9)
    println(map2)
    map2 - "lisi"
    println(map2)

  }

  /**
   * 14.1  iterator迭代器
   * scala针对每一类集合都提供了一个迭代器（iterator）用来迭代访问集合
   *
   * 使用迭代器遍历集合
   * 使用iterator方法可以从集合获取一个迭代器
   * 迭代器的两个基本操作
   * hasNext——查询容器中是否有下一个元素
   * next——返回迭代器的下一个元素，如果没有，抛出NoSuchElementException
   * 每一个迭代器都是有状态的(只能用一次, 内部指针只走一次, 走到最后就结束了, 不会再回到开头, 除非你再取得一个新的迭代器)
   * 迭代完后保留在最后一个元素的位置
   * 再次使用则抛出NoSuchElementException
   * 可以使用while或者for来逐个返回元素
   */
  def iterator_example(): Unit = {
    println("===========迭代器===========")

    // 定义一个列表，包含以下元素：1,2,3,4,5
    //使用while循环和迭代器，遍历打印该列表
    val list = List(1,2,3,4)
    println(list)
    val iter = list.iterator
    while(iter.hasNext){
      println(iter.next())
    }

    // 示例
    //
    //定义一个列表，包含以下元素：1,2,3,4,5
    //使用for 表达式和迭代器，遍历打印该列表
    val list1 = List(1,2,3,4,5)
    println(list1)
    for (elem <- list1) {
      println(elem)
    }
  }


  /**
   * 我们将来使用Spark/Flink的大量业务代码都会使用到函数式编程。下面的这些操作是学习的重点。
   * 现在我们将会逐渐接触函数式编程的方式.
   * 比如我们要说的第一个foreach方法, 就是一个典型的函数式编程方式.
   * 我们将一个函数当做参数 传递给另一个方法/函数
   *
   *
   * 遍历（foreach）
   * 映射（map）
   * 映射扁平化（flatmap）
   * 过滤（filter）
   * 是否存在（exists）
   * 排序（sorted、sortBy、sortWith）
   * 分组（groupBy）
   * 聚合计算（reduce）
   * 折叠（fold）
   */
  def function_programming_example(): Unit = {
    println("============函数式编程============")

    // 遍历 | foreach
    //之前，学习过了使用for表达式来遍历集合。我们接下来将学习scala的函数式编程，使用foreach方法来进行遍历、迭代。它可以让代码更加简洁。
    //
    //用途:
    //
    //foreach 会帮我们对集合中的每一个元素取出来进行处理, 处理的逻辑由我们自行定义
    // 示例
    //有一个列表，包含以下元素1,2,3,4，请使用foreach方法遍历打印每个元素
    val a = List(1,2,3,4)
    val func = (x:Int)=>println(x)
    a.foreach(func)
    a.foreach(x=>println(x))
    a.foreach(println(_))


    // 十六、映射 | map
    //集合的映射操作是将来在编写Spark/Flink用得最多的操作，是我们必须要掌握的。因为进行数据计算的时候，就是一个将一种数据类型转换为另外一种数据类型的过程。
    //
    //map方法接收一个函数，将这个函数应用到每一个元素，返回一个新的列表
    //
    //和foreach不同的是, map将处理好的元素封装到新的列表中, 并返回
    //
    //而foreach不会返回我们新的列表
    //
    //所以一般视使用场景, 来选择带返回的map还是不返回的foreach
    // 用法
    //方法签名
    //def map[B](f: (A) ⇒ B): TraversableOnce[B]
    // 案例一
    //创建一个列表，包含元素1,2,3,4
    //对List中的每一个元素加1
    println(a)
    println(a.map((x: Int) => x + 1))


    // 扁平化映射 | flatMap
    //扁平化映射也是将来用得非常多的操作，也是必须要掌握的。
    // 定义
    //可以把flatMap，理解为先map，然后再flatten
    // 就是说, 我们对待处理列表, 正常我们处理它 需要 先对其进行map操作, 然后再进行flatten操作 这样两步操作才可以得到我们想要的结果.
    //如果我们有这样的需求, 我们就可以使用flatMap( 此方法帮我们实现 先map 后flatten的操作)


  }


}
