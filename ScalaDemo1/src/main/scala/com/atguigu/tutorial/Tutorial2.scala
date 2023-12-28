package com.atguigu.tutorial

import java.io.{FileNotFoundException, IOException}
import java.time.DayOfWeek

object Tutorial2 {
  /**
   * 来源：
   *      https://mp.weixin.qq.com/s/u7dxCurLHlCGmZc6kBEKcQ
   *
   *  1. 计算机语言
   *          语言可以分为【编译型】、【解释型】：
   *          编译型：C
   *          解释型：Python
   *          对于 Java ：
   *          需要编译，代码需要有编译器编译成字节码
   *          解释执行/直接执行
   *          可移动性，代码一次编译，到处运行，【JVM才是核心】！！！
   *          对于 C 语言：
   *          可移植性，代码对应于不同平台，需要各自编译
   *          【编译器】：编译型、解释型的根本区别在哪？
   *          是否是强类型，什么是类型 —— 宽度
   *          语法 =》编译器 =》字节码《= JVM规则
   *          编译器衔接人和机器
   *          Java 其实不值钱，最值钱的是 JVM。
   *          JVM 由 C 语言编写，字节码(二进制) =》JVM(堆内/外(二进制)) =》 kernel(mmap, sendfile) 更快！！
   *          【语言模型】：
   *          面向过程的：第一类值 —— 基本类型 + 指针
   *          面向对象的：第一类值 —— 基本类型 + 对象类型
   *          函数式的：第一类值 —— 基本类型 + 对象类型 + 函数
   *
   * 2. Scala
   * Scala combines object-oriented and functional programming in one concise, high-level language.
   * Scala's static types help avoid bugs in complex applications, and its JVM and JavaScript runtimes
   * let you build high-performance systems with easy access to huge ecosystems of libraries.
   * Scala 是一种高级语言，其将【面向对象】和【函数式编程】相结合。Scala 的静态类型有助于避免复杂应用程序中的 Bug。
   * 同时它的 JVM 和 JavaScript 运行时允许构建高性能系统，可以方便的访问庞大的库生态系统。
   *
   *
   * 2.1 Scala 的 六大特点
   * SEANLESS JAVA INTEROP：Scala 运行在 JVM 上，因此 Java 代码和 Scala 代码可以相互混合，实现完全无缝集成；
   * TYPE INFERENCE：Don't work for the type system，Let the type system work for you。
   * CONCURRENCY & DISTRIBUTION：对集合进行并行操作，使用 actors 进行并发和分发，以及 futures 的异步编程
   * TRAITS：将 JAVA 风格接口的灵活性和类的强大功能结合起来
   * PATTERN MATCHING：类似于"Switch"，实现类的结构、序列、常量匹配
   * HIGER-ORDER FUNCTIONS：函数是第一类值，函数可以是变量，也可以传递给其他函数
   *
   *
   */
  def main(args: Array[String]): Unit = {
    flow_control()
    class_example()
    collection_example()
    functional_programming()
  }

  def flow_control(): Unit = {
    println("flow_control")
    // 1 if/else
    //Scala 的 if/else 控制结构类似于 Java 中的 if/else：
    //
    //if (条件1) {
    //
    //} else if (条件2) {
    //
    //} else {
    //
    //}
    //if 结构总是会返回一个结果，这个结果可以选择忽略，也可以将结果赋给一个变量，写作三元运算符：
    //
    val a = 1
    val b = 2
    val x = if (a < b) a else b
    println(x)
    //【面向表达式编程】
    //
    //当编写的每个表达式都返回一个值时，这种风格被称为面向表达式的编程或 EOP
    //
    //相反，不返回值的代码被称为语句，用于产生其他效果

    // 5.2 模式匹配
    //Scala 中有 match 表达式，类似于 Java 中的 switch 语句：
    // match 表达式也可以返回值，可以将字符串结果赋给一个新值：
    val candidate = 3
    val result = candidate match {
      case 1 => "one"
      case 2 => "two"
      case _ => "not 1 or 2"
    }
    println(result)
    //match 表达式可以用于任何数据类型：
    def getClassAsString(x: Any): String = x match {
      case s: String => s + " is a String"
      case i: Int => "Int"
      case f: Float => "Float"
      case l: List[_] => "List"
      case _ => "Unknown"
    }
    println(getClassAsString(candidate))
    // match 表达式支持在一个 case 语句中处理多个 case，下面的代码演示了将 0 或空字符串计算为 false：
    def isTrue(a: Any) = a match {
      case 0 | "" => false
      case _ => true
    }
    println(isTrue(candidate))
    println(isTrue(result))
    println(isTrue(""))
    // 可以看到这里输入参数 a 被定义为 Any 类型，这是所有 Scala 类的根类，就像 Java 中的 Object。
    //
    //在 case 语句中使用 if 表达式可以表达强大的模式匹配：
    // val count = 1
    val count = -8
    val res1 = count match {
      case 1 => println("one, a lonely number")
      case x if x == 2 || x == 3 => println("two's company, three's a crowd")
      case x if x > 3 => println("4+, that's a party")
      case _ => println("i'm guessing your number is zero or less")
    }

    // 5.3 异常捕获
    //Scala 的异常捕获由 try/catch/finally 结构完成：
    try {
      println("异常捕获")
    }catch {
      case fnfe: FileNotFoundException => println(fnfe)
      case ioe: IOException => println(ioe)
    }finally {
      println("end")
    }

    // 5.4 for 循环
    //Scala 中的 for 循环用来迭代集合中的元素，通常这样写：
    val args = Array(1,2,3,4,5)
    for (arg <- args) println(arg)
    for (i <- 0 to 5) println(i)
    for (i <- 0 to 10 by 2) println(i)

    // 在 for 循环中还可以使用 【yield】 关键字，从现有集合创建新集合：
    val res3 = for (i <- 1 to 5) yield i * 2
    println(res3)

    //在 yield 关键字之后使用代码块可以解决复杂的创建问题：
    //
    //val capNames = for (name <- names) yield {
    //  val nameWithoutUnderscore = name.drop(1)
    //  val capName = nameWithoutUnderscore.capitalize
    //  capName
    //}
    //
    //// 简约格式
    //val capNames = for (name <- names) yield name.drop(1).capitalize
    //还可以向 for 循环中添加守卫代码，实现元素的过滤操作：
    //
    //val fruit = for{
    //  f <- fruits if f.length > 4
    //} yield f.length
    var i = 0
    while (i<10){
      println("current: " + i)
      i += 1
    }
  }

  def class_example(): Unit = {
    println("class=============")
    //Scala 中的类与 Java 中类似：
    class Person(var firstName: String, var lastName: String) {
      def printFullName() = println(s"$firstName $lastName")
    }
    val person = new Person("zhang", "san")
    println(person.firstName)
    person.lastName = "si"
    person.printFullName()

    // 6.4 枚举类
    //枚举是创建小型常量组的工具，如一周中的几天、一年中的几个月等。
    sealed trait DayOfWeek
    case object Sunday extends DayOfWeek
    case object Monday extends DayOfWeek
    case object Tuesday extends DayOfWeek
    case object Wednesday extends DayOfWeek
    case object Thursday extends DayOfWeek
    case object Friday extends DayOfWeek
    case object Saturday extends DayOfWeek
    println(Monday)
    //println(DayOfWeek)

    // 6.6 样例类
    //样例类使用 case 关键字定义，它具有常规类的所有功能：
    //默认情况下，样例类的构造参数是公共 val 字段，每个参数会生成访问方法
    //apply 方法是在类的伴生对象中创建的，所以不需要使用 new 关键字创建实例
    //unapply 方法对实例进行解构
    //类中会生成一个 copy 方法，在克隆对象或克隆过程中更新字段时非常有用
    // 类中会生成 equals 和 hashcode 方法，用于比较对象
    //生成默认的 toString 方法
    case class BaseballTeam(name: String, lastWorldSeriesWin: Int)
    val cubs1908 = BaseballTeam("Chicago Cubs", 1908)
    val cubs2016 = cubs1908.copy(lastWorldSeriesWin = 2016)
    println(cubs1908)
    println(cubs2016)
    println(cubs1908.equals(cubs2016))
    println(cubs1908.hashCode())
    println(cubs2016.hashCode())


    // 6.7 样例对象
    //样例对象使用 case object 关键字定义：
    //
    //可以序列化
    //
    //有一个默认的 hashcode 实现
    //
    //有一个 toString 实现
    //
    //样例对象主要用于：
    //
    //创建枚举
    sealed trait Topping
    case object Cheese extends Topping
    case object Pepperoni extends Topping
    case object Sausage extends Topping
    case object Mushrooms extends Topping
    case object Onions extends Topping

    sealed trait CrustSize
    case object SmallCrustSize extends CrustSize
    case object MediumCrustSize extends CrustSize
    case object LargeCrustSize extends CrustSize

    sealed trait CrustType
    case object RegularCrustType extends CrustType
    case object ThinCrustType extends CrustType
    case object ThickCrustType extends CrustType

//    case class Pizza(
//      crustSize: CrustSize,
//      crustType: CrustType,
//      toppings: Seq[Topping]
//    )
//    val p = Pizza()
//    println(p)


    // 8. Traits
    //Scala 中 Traits 类似于 Java 中的接口(interface)，它可以将代码分解成小的模块化单元，实现对类的扩展。
     """
      |Java中的接口（interface）和抽象类（abstract class）是两种不同的类类型。
      |1. 程序结构上的区别：
      |   - 接口只有方法的声明，没有方法的实现，所有的方法都是抽象的，默认为public访问修饰符。接口中可以定义常量。
      |   - 抽象类是包含了方法的声明和实现，并且可以有普通方法（非抽象方法），可以定义成员变量。抽象类中可以定义抽象和非抽象方法。
      |2. 使用上的区别：
      |   - 接口用于定义一组相关的方法，通过实现接口的类来完成方法的具体实现。一个类可以实现多个接口，实现接口使用implements关键字。
      |   - 抽象类通常用于定义具有相似行为和属性的类。一个类只能继承一个抽象类，使用extends关键字。
      |3. 设计思想上的区别：
      |   - 接口体现了"行为"的抽象，表示一种能力或者约束。通过接口可以实现多态和解耦。
      |   - 抽象类体现了" is-a "的关系，表示一种继承关系，封装了共性的属性和行为。
      |总的来说，接口用于定义一组相似的行为或能力，实现类实现接口并提供具体的实现。抽象类则是用于表示一种共性的类，并且可以包含具体的实现。
      |""".stripMargin


    trait Speaker {
      def speak(): String // 这是一个抽象方法
    }
    trait TailWagger {
      def startTail(): Unit = println("tail is wagging")
      def stopTail(): Unit = println("tail is stopped")
    }
    class Dog(val name: String) extends Speaker with TailWagger {
      def speak(): String = "Woof!"
    }
    val d = new Dog("xiaotianquan")
    println(d.speak())
    d.startTail()
    d.stopTail()
     println(d.name)
    // 上面的代码中，我们使用 extends 关键字和 with 关键字为 Dog 类扩展了 speak 方法。
    // todo scala 中 extends with 两个关键词的区别
    //    extends 用于继承一个trait接口
    //    with 对于具有具体方法的 trait，可以在创建实例时将其混合：

    // 8.1 trait
    //定义一个 trait，它具有一个具体方法和一个抽象方法：
    //
    trait Pet {
      def speak = println("Yo")
      def comToMaster(): Unit
    }
    // 当一个类扩展一个 trait 时，每个抽象方法都必须实现：
    class Dog1(name:String) extends Pet{
      override def comToMaster(): Unit = println("Woo-hoo, I'm coming!")
    }
    val d1 = new Dog1("hh")
    d1.comToMaster()
    // 对于具有具体方法的 trait，可以在创建实例时将其混合：
    val d2 = new Dog1("hashiqi") with TailWagger
    d2.comToMaster()
    d2.startTail()
    d2.stopTail()

    class Dog2(name:String) extends TailWagger
    val d3 = new Dog2("tedy")
    println(d3)
    d3.startTail()
    d3.stopTail()

    // 8.2 重写方法
    //类可以覆盖在 trait 中已定义的方法：
    class Cat extends Pet {
      override def speak(): Unit = println("meow")
      //def comToMaster(): Unit = println("That's not happen.")
      // 可以省略override关键字
      override def comToMaster(): Unit =  println("That's not happen.")
    }
    val c1 = new Cat()
    c1.speak()
    c1.comToMaster()

    // todo unapply方法
    class Person05(val name: String, var age: Int) {}
    object Person05 {
      // 创建对象的方法
      def apply(name: String, age: Int): Person05 = new Person05(name, age)

      // 解析对象的方法
      // unapply方法(对象提取器)，user作为unapply方法的参数，unapply方法将user对象的name和age属性提取出来
      def unapply(arg: Person05): Option[(String, Int)] = {
        // 如果解析的参数为null
        if (arg == null) None else Some((arg.name, arg.age))
      }
    }
    val p1 = Person05("yuanx", 26)
    println(p1)
    val up1 = Person05.unapply(p1)
    println(up1)

    // 8.3 抽象类
    //Scala 中的抽象类使用较少，一般只在以下情况使用：
    //希望创建一个需要构造函数参数的基类
    //Scala 代码将被 Java 代码调用
    abstract class Pet2(name: String) {
      def speak(): Unit = println("Yo")
      def comeToMaster(): Unit
    }

  }


  def collection_example(): Unit = {
    println("collection_example")
    """
      |Class
      |
      |描述
      |
      |ArrayBuffer
      |
      |有索引的、可变序列
      |
      |List
      |
      |线性链表、不可变序列
      |
      |Vector
      |
      |有索引的、不可变序列
      |
      |Map
      |
      |键值对
      |
      |Set
      |
      |无序的、去重集合
      |
      |Map 和 Set 都有可变和不可变两种版本
      |""".stripMargin

    // List 是一个线性的、不可变的序列，即一个无法修改的链表，想要添加或删除 List 元素时，都要从现有 List 中创建一个新 List。
    // 因为 List 是不可变的，只能通过创建一个新列表来象现有 List 中添加或追加元素：
    // 如果需要在一个不可变序列中添加元素，可以使用 Vector
    //因为 List 是一个链表类，所以不要通过索引值访问大型列表元素，可以使用 Vector 或 ArrayBuffer
     // todo  List底层数据结构是链表，Vector底层数据结构是数组
    //        链表 查询慢、增删快        数组查询快、增删慢

    // 9.4 Vector
    //Vector 是一个索引的、不可变序列，索引意味着可以通过索引值快速访问，其他方面与 List 类似：
    val nums = Vector(1, 2, 3, 4, 5)
    val strings = Vector("one", "two")

   class Person3(name: String)
    val peeps = Vector(
      new Person3("Bert"),
      new Person3("Ernie"),
      new Person3("Grover")
    )

    for (elem <- nums) {
      println(elem)
    }
    for (elem <- strings) {println(elem)}
    for (elem <- peeps) {println(elem)}

    // 由于 Vector 是不可变的，只能通过创建新序列的方式来向现有 Vector 追加元素：
    val a1 = Vector(1,2,3)
    val b1 = a1 :+ 4
    val b2 = a1 ++ Vector(4, 5)
    println(b1)
    println(b2)

    val b3 = 0 +: a1
    val b4 = Vector(-1, 0) ++: a1
    println(b3)
    println(b4)
    println(a1)

  }


  def functional_programming(): Unit = {
    println("functional_programming")
    """
      |函数式编程是一种强调只使用纯函数和不可变值编写应用程序的编程风格。
      |纯函数应该有以下特点：
      |函数的输出只取决于它的输入变量
      |函数不会改变任何隐藏状态
      |函数没有任何“后门”：不从外部世界（控制台、Web服务、数据库、文件等）读取数据，也不向外部世界写出数据
      |
      |Scala 中 "scala.math._package"中的如下方法都是纯函数：
            |abs
            |ceil
            |max
            |min
      |当然，Scala 中有很多非纯函数，如 foreach：
      |集合类的 foreach 函数是不纯粹的，它具有其副作用，如打印到 STDOUT
      |它的方法签名声明了返回 Unit 类型，类似的，任何返回 Unit 的函数都是一个不纯函数
      |非纯函数通常有以下特点：
            |读取隐藏的输入，即访问没有显示传递到函数中作为输入参数的变量和数据
            |写出隐藏输出
            |修改给定的参数
            |对外部世界执行某种I/O
      |""".stripMargin


    // 11.2 传递函数
    //函数可以作为变量进行传递，允许将函数作为参数传递给其他函数。
    //
    val nums = Vector(1,2,3)
    val doubles = nums.map(_ * 2)
    println(doubles)


    // 11.3 没有 Null 值
    //todo 在 Scala 中，没有 null 的存在，使用 Option/Some/Nothing 这样的结构进行处理。
      //Some 和 Nothing 是 Option 的子类
      //通常声明一个函数返回一个 Option 类型
      //接收到可以处理的参数则返回 Some
      //接收到无法处理的参数则返回 Nothing
    def toInt(s: String): Option[Int] = {
      try{
        Some(Integer.parseInt(s.trim))
      } catch {
        case e: Exception => None
      }
    }
    val x = "a"
    val res1 = toInt(x)
    println(toInt("1"))
    println(res1)
    toInt(x) match {
      case Some(i) => println(i)
      case None => println("That didn't work.")
    }

    val stringA = "1"
    val stringB = "2"
    val stringC = "a"
    val y = for {
      a <- toInt(stringA)
      b <- toInt(stringB)
      c <- toInt(stringC)
    } yield a + b + c
    println(y)
    //当三个字符串都转换为整数时，则返回一个包装在 Some 中的整数
    //当三个字符串中任何一个不能转换为整数时，则返回一个 None

    """
      |在 Scala 中，Option/Some/Nothing 是用于处理可能存在或不存在值的类型。
      |
      |- Option 是一个容器类型，代表一个可能存在的值或者不存在的值。它有两个子类型：Some 和 None。Some 表示存在一个非空值，而 None 表示不存在值。
      |
      |- Some 是 Option 的子类型，用于表示存在一个非空值的情况。Some 包装了实际的值。
      |
      |- Nothing 是 Scala 类型系统的底层类型，也是所有类型的子类型。它表示永远不存在的值。通常用于表示异常或不可能到达的代码路径。
      |
      |这些类型在 Scala 中的主要作用是避免使用 null 来表示不存在的值。使用 Option/Some/None 可以更好地表达代码的意图，并在编译期间进行静态检查，避免空指针异常。
      |""".stripMargin

  }






}
