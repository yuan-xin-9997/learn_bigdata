package com.atguigu.day02
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
class $02_RDDPartitionTest {
  val conf = new SparkConf()
    .setMaster("local[*]").setAppName("test")
    .set("spark.testing.memory", "2147480000")
    .set("spark.default.parallelism", "10")
  val sc = new SparkContext(conf)

  /**
   * RDD的不同创建方式，所创建的分区数量：
   *    1. 通过本地集合创建
   *       2. 通过读取文件创建
   *       3. 通过其他的RDD衍生
   *
   * @param args
   */

  /**
   * 1. 通过本地集合创建，其RDD分区数的数量：
   *      如果nullSlices可以手动设置分区数，分区数=设置的值（优先级最高）
   *      如果没有设置nullSlices，此时分区数=defaultParallelism
   *          如果在SparkConf中设置spark.default.parallelism，此时defaultParallelism=spark.default.parallelism
   *          如果在SparkConf中没有设置spark.default.parallelism，此时defaultParallelism=totalCores
   *              此时如果master=local，defaultParallelism=1
   *              master=local[N]，defaultParallelism=N
   *              master=local[*]，defaultParallelism=CPU逻辑核数
   *              master=spark://...(集群模式)  此时defaultParallelism=math.max(所有executor的总核数, 2)
   */
   @Test
  def createRDDByCollection(): Unit = {
    println(".......")

    val list = List(1, 4, 3, 6)
    val rdd = sc.makeRDD(list)
    println(rdd.collect())
    println(rdd.collect().toList)

//    val rdd2 = sc.parallelize(list, 8)
    val rdd2 = sc.parallelize(list)
    println(rdd2.collect().toList)

    // 查看RDD的分区数，nullSlices可以手动设置分区数
    println(rdd2.getNumPartitions)
  }

  /**
   * sc.textFile(
       * path: String,
       * minPartitions: Int = defaultMinPartitions)
   * 2. 通过读取文件创建，其RDD分区数的数量
   *    >= minPartitions
   *    defaultMinPartitions: Int = math.min(defaultParallelism, 2)
   *    读取文件创建的RDD分区数最终由文件切片决定
   */
    @Test
  def createRDDByFile(): Unit = {
    println("------------------")
    // val rdd = sc.textFile("datas/wc.txt")
    val rdd = sc.textFile("datas/wc.txt", 4)
    println(rdd.collect())
    println(rdd.collect().toList)

    println(rdd.getNumPartitions)

    //    System.setProperty("user.name", "atguigu")

    //    System.setProperty("HADOOP_USER_NAME", "atguigu");
    // val rdd2 = sc.textFile("hdfs://hadoop102:8020/user/hive/warehouse/user_session")
    val rdd2 = sc.textFile("hdfs://hadoop102:9820/user/hive/warehouse/user_session")
    println(rdd2.collect())
    println(rdd2.collect().toList)

    println(rdd2.getNumPartitions)
  }

  /**
   * 3. 通过其他的RDD衍生
   */
  def createRDDByRDD(): Unit = {
    println("-------------------")
    val rdd = sc.textFile("datas/wc.txt")

    val rdd2 = rdd.flatMap(x => x.split(" "))
    println(rdd2.collect().toList)
  }

}
