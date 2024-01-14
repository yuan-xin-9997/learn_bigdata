package com.atguigu.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object $06_Convert {
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  val spark = SparkSession.builder()
      .appName("spark-sql")
      .master("local[4]")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
  import spark.implicits._

  /**
   * todo  RDD、DataFrame、DataSet相互转换
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val list: List[Person] = List(
      Person(1, "zhangsan", "man", 11),
      Person(2, "lisi", "woman", 12),
      Person(3, "wangwu", "man", 13),
      Person(4, "wangwu", "man", 25),
      Person(5, "wangwu", "man", 100),
      Person(5, "wangwu", "man", 100)
    )

    // todo 1. RDD to DataFrame
    val rdd: RDD[Person] = spark.sparkContext.parallelize(list)
    val df: DataFrame = rdd.toDF()

    // todo 2. RDD to DataSet
    val ds: Dataset[Person] = rdd.toDS()

    // todo 3. DataFrame to RDD[Row]
    val rdd1: RDD[Row] = df.rdd
    //  todo Row类型的取值   getAs[列类型](角标)
    //  todo Row类型的取值   getAs[列类型](列名)
    val rdd3 = rdd1.map(row => {
      // val age = row.getAs[Int](3)
      val age = row.getAs[Int]("age")
      // val name = row.getAs[String](1)
      val name = row.getAs[String]("name")
      (name, age)
    })
    println(rdd3.collect().toList)


    // todo 4. DataSet to RDD
    val rdd2: RDD[Person] = ds.rdd
    val rdd5 = rdd2.map(person => (person.name, person.age))
    println(rdd5.collect().toList)

    // todo 5. DataFrame to DataSet:  需要指定行类型   df.as[行类型]
    //      行类型可以写元组，元组的元素类型必须与列的类型对应，或者能够自动转换
    //      行类型可以写样例类，样例类的元素类型必须与列的类型对应，或者能够自动转换
    val ds2 = df.as[Person]    // todo 行类型可直接写Person
    ds2.show()

    val df7: DataFrame = spark.read.json("datas/test.json")
    df7.show()
    df7.printSchema()
    // val ds8: Dataset[(String, Int, String)] = df7.as[(String, Int, String)]  // 报错  Cannot up cast `name` from string to int.
    val ds8: Dataset[(Long, String, String)] = df7.as[(Long, String, String)]  // todo 行类型可以写元组，元组的元素类型必须与列的类型对应，或者能够自动转换
    ds8.show()

    val ds9 = df7.as[AAA]  // todo 行类型可以写样例类，属性名必须与json中的key保持一致，顺序可不一致
    ds9.show()
    // ds9.map(x=>x.age)

    val ds10 = df7.as[AAA2] // todo 行类型可以写样例类，不带属性的，但是不推荐这种写法，因为没有属性，不方便函数计算
    ds10.show()
    // ds10.map(x=>x.age)

    // todo 6. DateSet to DataFrame: ds.toDF
    val df3: DataFrame = ds.toDF()
    df3.show()

  }
}

case class AAA(age:Long, sex:String, name:String)  // todo 此处属性名必须与json中的key保持一致，顺序可不一致。属性名也可不写
case class AAA2()  // todo 属性名也可不写
