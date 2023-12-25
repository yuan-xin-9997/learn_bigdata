object HelloScala {
  def main(args: Array[String]): Unit = {
    // java的方法调用
    System.out.println("hello scala")

    // scala的方法调用
    println("hello scala")

    val v = for(i <- 1 to 10) yield i * 10
    println(v)
  }
}
