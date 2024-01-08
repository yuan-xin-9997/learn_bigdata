package com.atguigu.day04

import com.mysql.jdbc.PreparedStatement
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.junit.Test

import java.sql
import java.sql.{Connection, DriverManager}

/**
 * 行动算子是触发了整个作业的执行。因为转换算子都是懒加载，并不会立即执行。
 */
class $03_Action {
  val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  /**
   * collect()以数组的形式返回数据集
   *      收集RDD每个分区的数据，以数组形式封装之后发给Driver
   *      如果Driver和Task不在一起，则需要序列化进行网络传输
   *      todo collect可能出现的问题
   *          如果RDD加起来的数据量超过了Driver的内存（默认1G），可能出现内存溢出，工作中一般需要将Driver内存设置为5-10G
   *          可以通过bin/spark-submit --driver-memory 10G
   *      适用于广播变量，大表join小表的时候
   */
  @Test
  def collect(): Unit = {
    val rdd = sc.parallelize(List(1, 4, 2, 6, 8, 0))

    val arr: Array[Int] = rdd.collect()

    println(arr.toList)
  }

  /**
   * count: 统计RDD元素个数
   */
  @Test
  def count(): Unit = {
    val rdd = sc.parallelize(List(1, 4, 2, 4, 6, 8))
    println(rdd.count())
//    rdd.size
  }

  /**
   * first:获取RDD第一个元素
   *    RDD如果有多个分区，那获取的事哪个分区的元素：实际上先启动1个job，从0号分区的第1个元素，
   *    如果没有获取到元素，则再启动1个job，按分区号依次获取，直到获取到第一个元素
   *    总共只会启动2个job
   *
   */
  @Test
  def first(): Unit = {
    val rdd = sc.parallelize(List(2, 4, 2, 6, 8, 7), 3)
    // println(rdd.first())

    val rdd2 = rdd.map(x => (x, null)).partitionBy(new HashPartitioner(10))
    println(rdd2.first())

    Thread.sleep(1000000)
  }

  /**
   * take(num: Int) 获取RDD前num个元素
   *      先启动1个job，从0号分区的前num个元素，如果0号分区数量不足，再从剩下分区获取剩下的元素
   */
  @Test
  def take(): Unit = {
    val rdd1 = sc.parallelize(List(0, 1, 2, 3, 8, 7), 3)

//    rdd1.mapPartitionsWithIndex((index, it)=>{
//      println(s"index=${index} -- data=${it.toList}")
//      it
//    }).collect()

    println(rdd1.take(1).toList)

    Thread.sleep(1000000)
  }

  /**
   * takeOrdered(num: Int) 返回 排序后前n个元素组成的数组
   *    todo 注意 没有shuffle操作，只启动1个job
   *        可以在每个分区里面先排序取前num个元素，再聚集在一起排序取前num个元素，不存在shuffle过程
   *
   * todo 疑惑 task这些action算子，完全可以有transformation算子替代？
   */
  @Test
  def takeOrdered(): Unit = {
    val rdd1 = sc.parallelize(List(0, 1, 2, 3, 8, 7), 3)

    val rdd2 = rdd1.takeOrdered(3)

    println(rdd2.toList)

    Thread.sleep(1000000)
  }

  /**
   * countByKey:统计每种key的个数
   *    底层调用：self.mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap
   */
  @Test
  def countByKey(): Unit = {
      val rdd = sc.parallelize(List("a"->10, "b"->20, "c"->30, "d"->40))

    val rdd2: collection.Map[String, Long] = rdd.countByKey()

    println(rdd2.toList)
  }

  /**
   * 保存结果为文件API
   */
  @Test
  def save(): Unit = {
    // saveAsTextFile(path)保存成Text文件
    //	（1）函数签名
    //（2）功能说明：将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，对于每个元素，Spark将会调用toString方法，将它装换为文件中的文本

    val rdd = sc.parallelize(List("a"->10, "b"->20, "c"->30, "d"->40), 2)
    rdd.saveAsTextFile("datas/res")
  }

  /**
   * foreach(func:RDD元素类型=>Unit)=>Unit：遍历RDD中每一个元素
   *      底层调用scala的foreach函数
   *      func函数针对元素进行操作，函数执行次数=元素个数
   */
  @Test
  def foreach(): Unit = {
    val rdd = sc.parallelize(List("a"->10, "b"->20, "c"->30, "d"->40), 2)

    rdd.foreach(println)
  }

  /**
   * foreachPartition(func:Iterator[元素RDD元素类型]=》Unit)=>Unit: 针对每个分区进行遍历
   *       func函数针对分区进行操作，函数执行次数=分区个数
   *       使用场景：用于将数据写入mysql、hbase、redis，可以减少连接的创建与销毁次数
   *
   * 对比记忆
   * mapPartitions(func:Iterator[RDD元素类型]=>Iterator[B])：一对一转换，原RDD一个分区计算得到新RDD一个分区
   *      里面的函数func是针对每个分区操作，分区有多少个，函数执行多少次
   *      使用场景：一般用于从MySQL/hbase/redis查询数据，可以减少连接创建与销毁
   */
  @Test
  def foreachPartition(): Unit = {
    val rdd = sc.parallelize(List("a"->10, "b"->20, "c"->30, "d"->40), 2)

    // 初始做法
//    rdd.foreach(x=>{
//      // 创建connection
//      // ...
//      // 关闭连接
//      // ..
//    })

    // 推荐做法
    rdd.foreachPartition(it=>{
      var connection:Connection=null
      var statement:sql.PreparedStatement=null
      try {
        connection=DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root")
        statement = connection.prepareStatement("insert into wc values(?,?)")

        var i= 0
        it.foreach(x=>{
          statement.setString(1, x._1)
          statement.setInt(2, x._2)
          // todo 添加到一个批次中
          statement.addBatch()
          if(i%1000==0){
            // 执行批次
            statement.executeBatch()
            // 清空批次
            statement.clearBatch()
            i+=1
          }
          // 执行剩余批次，不满1000的批次
          statement.executeBatch()
        })
      }catch {
        case e:Exception=>e.printStackTrace()
      }finally {
        if(statement!=null)
          statement.close()
        if(connection!=null)
          connection.close()
      }
    })

  }

}
