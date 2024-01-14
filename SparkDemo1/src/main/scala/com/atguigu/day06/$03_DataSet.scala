package com.atguigu.day06
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.junit.Test




class $03_DataSet {

  val spark = SparkSession.builder()
    .appName("spark-sql")
    .master("local[4]")
    .config("spark.testing.memory", "2147480000")
    .getOrCreate()

  import spark.implicits._

  /**
   * DataFrame的创建方式：
   *       1、通过toDS的方式创建
   *       2、通过读取文件创建
   *       3、通过其他的DataSet衍生
   *       4、通过createDataSet方法创建
   *
   */

  /**
   * 1、通过toDS的方式创建
   *        rdd.toDS
   *        集合.toDS
   *            集合/RDD中元素类型是样例类的时候，转成DataFrame之后，列名默认就是属性名
   *            集合/RDD中元素类型是元组的时候，转成DataFrame之后，列名默认就是_N
   */
  @Test
  def createDataSetBytoDS(): Unit = {
    // ------------------------集合.toDS----------------------------
    val list: List[Person] = List( Person(1, "zhangsan", "man", 11),  Person(2, "lisi", "woman", 12), Person(3, "wangwu", "man", 13) )

    val ds: Dataset[Person] = list.toDS()

    // 展示数据，结果是二维表，
    ds.show()

    // 展示schema，列信息
    ds.printSchema()

    val list2: List[(Int, String, String, Int)] = List((1, "zhangsan", "man", 11), (2, "lisi", "woman", 12), (3, "wangwu", "man", 13))
    val ds1 = list2.toDS() // 不传列名
    ds1.show
    ds1.printSchema

    // DataSet to DataFrame
    val df1: DataFrame = list2.toDS().toDF("ID", "NAME", "SEX", "AGE") // 不传列名
    df1.show
    df1.printSchema

    // ------------------------RDD.toDS----------------------------
    println("------------------------RDD.toDS----------------------------")
    val rdd = spark.sparkContext.parallelize(list)

    val ds3 = rdd.toDS()
    ds3.show
    ds3.printSchema
  }

  /**
   * 2、通过读取文件创建
   *    todo 注意 一般通过读取文本文件生成DataSet，可以通过json，但是会生成如下结果
   *               +--------------------+
   *               |               value|
   *               +--------------------+
   *               |{"name": "lisi1",...|
   *               |{"name": "lisi2",...|
   *               |{"name": "lisi3",...|
   *               |{"name": "lisi4",...|
   *               +--------------------+
   */
  @Test
  def createDataSetByFile(): Unit = {

    val ds: Dataset[String] = spark.read.textFile("datas/wc.txt")
    ds.show

    val ds1: Dataset[String] = spark.read.textFile("datas/test.json")
    ds1.show
  }

  /**
   * 3、通过其他的DataSet衍生
   */
  @Test
  def createDataSetByDataSet(): Unit = {
    val ds: Dataset[String] = spark.read.textFile("datas/wc.txt")

    // 下面是一个WordCount案例

    val ds2: Dataset[String] = ds.flatMap(x => x.split(" "))
    ds2.show()

    val ds3: Dataset[(String, Int)] = ds2.map(x => (x, 1))
    ds3.show()

    // 类似于SQL的 select wc,sum(num) from ds3 group byn wc，    sparkSQL里面没有ReduceByKey
    val df2 = ds3.toDF("wc", "num").groupBy("wc").sum("num")
    df2.show()
  }

  /**
   * 4、通过createDataSet方法创建
   */
  @Test
  def createDataSetByMethod(): Unit = {
    val ds = spark.createDataset(List(1,3,5,6,7))
    ds.show()
  }

}
