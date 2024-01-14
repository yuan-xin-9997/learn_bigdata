package com.atguigu.day06

import org.apache.spark.sql.Dataset

object $08_Demo {

  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  val spark = SparkSession.builder()
      .appName("spark-sql")
      .master("local[4]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
  import spark.implicits._
  /**
   * 案例：双十一淘宝美妆数据
   *
   * 如果title中包含某一个类别的关键字，那么取出主类与子类补充到元数据
   * 如果title中出现了男士、男生、男关键字，那么为男士专用
   * 1、查看每个品牌的商品数量，降序排序
   * 2、查看每个品牌的销售量，降序排序
   * 2、查看每个品牌的销售额，降序排序
   * 3、求得每个主类的销量占比情况
   * 3、求得每个子类的销量占比情况
   * 4、各品牌各主类的总销售额
   * 4、各品牌各子类的总销售量
   * 5、各品牌商品的平均评论数
   * 6、各品牌商品的平均价格
   * 7、各品牌男性专用商品的销量情况
   * 7、各品牌男性专用商品的销售额情况
   * 8、每月的销量情况
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // todo 求得每个主类的销量占比情况

    // 读取数据
    val datasDF = spark.read.csv("datas/双十一淘宝美妆数据.csv").toDF("update_time", "id", "title", "price", "sale_count", "comment_count", "name")

    val rawTypeDS: Dataset[String] = spark.read.textFile("datas/type.txt")
    datasDF.show()
    rawTypeDS.show()

    // 针对type表进行处理，得到主类和子类一一映射的新DataFrame
    val typeDF: DataFrame = rawTypeDS.flatMap(line => {
      // line = 护肤品 乳液类 乳液 美白乳 润肤乳 凝乳 柔肤液 亮肤乳 菁华乳 修护乳
      val arr = line.split(" ")
      val mainType = arr(1)
      val subTypes = arr.tail.tail
      subTypes.map(x=>(x, mainType))
    }).toDF("kv", "main_type")
    typeDF.show()

    // 注册UDF函数
    spark.udf.register("contains", contains _)

    // 注册临时表
    datasDF.createOrReplaceTempView("sales")
    typeDF.createOrReplaceTempView("s_type")

    // join，使用row_number()函数进行开窗
    //    ROW_NUMBER()：会根据顺序计算，字段相同就按排头字段继续排
    // |     12668|      CHAND0/自然堂活泉保湿修护精...|爽肤水|   化妆水|  1|
    //|     12668|      CHAND0/自然堂活泉保湿修护精...|精华水|   精华类|  2|
    // 由于一个主类会有多个关键字的销售记录与之对应，因此使用row number对title进行开窗，并按照
    //   sale_count进行排序
    // todo 此处有问题，因为同一个title的销售记录，会对应多个主类
    val joinDF: DataFrame = spark.sql(
      """
        |select
        | a.sale_count,
        | a.title,
        | b.kv,
        | b.main_type,
        | row_number() over(partition by title order by sale_count) rn
        |from sales a join s_type b
        |on contains(a.title, b.kv)
        |""".stripMargin
    )
    joinDF.show()

    // 将join之后的结果注册成临时表
    joinDF.createOrReplaceTempView("t_tmp")

    // 按照rn=1，并且销售量不为null进行筛选
    val filterDF = spark.sql(
      """
        |select *
        |from t_tmp
        |where rn = 1 and sale_count is not null
        |""".stripMargin
    )
    filterDF.show()

    // 求每个主类的销售总量，所有主类的销售总量
    spark.sql(
      """
        |select
        | main_type,
        | sum(sale_count) sale_count
        |from t_tmp
        |where rn = 1 and sale_count is not null
        |group by main_type
        |""".stripMargin
    ).createOrReplaceTempView("t_tmp2")  // 注册临时表，按主类分组，求得每个主类的销售总量
    val totalSaleCountDF: DataFrame = spark.sql(
      """
        |select
        | main_type,
        | sale_count,
        | sum(sale_count) over() total_sale_count
        |from t_tmp2
        |""".stripMargin
    )
    totalSaleCountDF.show()
    totalSaleCountDF.createOrReplaceTempView("t_tmp3") // 注册临时表，求主类销售占比

    // 计算每个主类销售占比
    spark.sql(
      """
        |select
        | main_type,
        | sale_count / total_sale_count   ratio
        |from
        |t_tmp3
        |""".stripMargin
    ).show()

  }

  /**
   * UDF自定义函数，判断title中是否包含keyword
   * @param title
   * @param keyword
   * @return
   */
  def contains(title:String, keyword:String): Boolean = {
    val bool = title.contains(keyword)
    bool
  }
}
