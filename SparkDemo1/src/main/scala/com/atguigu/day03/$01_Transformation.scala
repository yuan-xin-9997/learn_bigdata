package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

class $01_Transformation{

  val conf = new SparkConf()
    .setMaster("local[4]").setAppName("test")
    .set("spark.testing.memory", "2147480000")
//    .set("spark.default.parallelism", "10")
  val sc = new SparkContext(conf)

  @Test
  def map(): Unit = {

    """
      |create database test;
      |create table person(
      | id int(11),
      | name varchar(25),
      | age int(11)
      |)
      |
      |insert into person values(1, 'zhangsan', 1);
      |insert into person values(2, 'zhangsan', 2);
      |insert into person values(3, 'zhangsan', 3);
      |""".stripMargin

    // RDD 中的元素是用户的id
//    val rdd = sc.parallelize(List(1,4,2,3,7,10))
    val rdd = sc.parallelize(List(1,10,3))

//    var connection: Connection = null
//    var statement: PreparedStatement = null
//    // 创建连接
//    connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root")
//    // 创建statement对象
//    statement = connection.prepareStatement("select * from person where id=?")

    println(s"${Thread.currentThread().getName} -- main ")

    // 需求：根据id从mysql查询数据获取用户详细的信息
    val rdd2 = rdd.map(id => {
      // 创建连接
      var connection: Connection = null
      var statement: PreparedStatement = null
      var name: String = null
      var age: Int = -1
      try {
        // 创建连接
        connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root")
        // 创建statement对象
        statement = connection.prepareStatement("select * from person where id=?")
        // 赋值
        statement.setInt(1, id)
        // 执行SQL
        val res: ResultSet = statement.executeQuery()
        while (res.next()) {
          name = res.getString("name")
          age = res.getInt("age")
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (statement != null)
          statement.close()
        if (connection != null)
          connection.close()
      }
      println(s"${Thread.currentThread().getName} -- id=${id} -- map ")
      (id, name, age)
    })

//    if (statement != null)
//      statement.close()
//    if (connection != null)
//      connection.close()

    println(rdd2.collect().toList)

    // todo 问题：map里面的函数是针对每个元素操作，每个元素都会创建、销毁，当数据量比较大的时候性能会特别慢
    //     解决方案: (1) 在map使用连接池[不可取，连接池的连接数有限，工作中分区数比较多，可能导致分区不能并行度不够，效率受影响]
    //             (2) 创建与销毁连接放在map函数体外部，此时貌似所有元素从mysql取数据时候可以使用同一连接
    //                     【Spark算子函数中代码是在task中执行，函数体外面的代码是在Driver中执行的，，所以如果task中使用了Driver的对象
    //                     需要序列化后传递给Task使用，但是Mysql Connection不能序列化】
    //              (3) 使用mapPartitions，创建的MySQL连接数量=分区数量，而map方法创建的连接数量=元素数量
  }

  /**
   * mapPartitions(func:Iterator[RDD元素类型]=>Iterator[B])：一对一转换，原RDD一个分区计算得到新RDD一个分区
   *      里面的函数func是针对每个分区操作，分区有多少个，函数执行多少次
   *      使用场景：一般用于从MySQL/hbase/redis查询数据，可以减少连接创建与销毁
   *
   *      map与mapPartitions的区别
   *       1. 函数针对的对象不一样
   *          map里面的函数是针对分区的每个元素操作
   *          mapPartitions里面的函数针对每个分区进行操作
   *       2. 函数返回值类型不易于
   *          map函数，返回新RDD的元素，新RDD元素个数=原RDD元素个数
   *          mapPartitions，返回新RDD分区所有数据，新RDD元素个数不一定 = 原RDD元素个数
   *       3. 元素内存回收的时机不一样
   *          map每个元素操作完成之后就可以进行垃圾回收
   *          mapPartitions必须等到分区迭代器遍历完成之后才会垃圾回收，如果RDD分区数据量比较大，可能会出现内存溢出，
   *              此时可以用map代替，map只是慢，但是靠谱，【完成比完美重要】
   */
  @Test
  def mapPartitions(): Unit = {
    """
      |create database test;
      |create table person(
      | id int(11),
      | name varchar(25),
      | age int(11)
      |)
      |
      |insert into person values(1, 'zhangsan', 1);
      |insert into person values(2, 'zhangsan', 2);
      |insert into person values(3, 'zhangsan', 3);
      |""".stripMargin

    // TODO 分区数推荐设置所有Executor核数2-3倍，因为Task数量等于当前stage最后一个RDD的分区数，如果分区数不变，那么Task数量刚
    //  好约是核数的2-3倍，比较适合

    // RDD 中的元素是用户的id
    //    val rdd = sc.parallelize(List(1,4,2,3,7,10))
    val rdd = sc.parallelize(List(1, 10, 3,4,5,6,7,8,9))
    println(rdd.getNumPartitions)

    println(s"${Thread.currentThread().getName} -- mapPartitions -- main ")

    // 需求：根据id从mysql查询数据获取用户详细的信息
    val rdd2 = rdd.mapPartitions(it=>{
        // 创建连接
        var connection: Connection = null
        var statement: PreparedStatement = null
        var name: String = null
        var age: Int = -1
        // 使用可变List封装结果对象
        var result:ListBuffer[(Int, String, Int)] = ListBuffer[(Int, String, Int)]()
        try {
          // 创建连接
          connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root")
          // 创建statement对象
          statement = connection.prepareStatement("select * from person where id=?")
          println(connection)
          // it是分区内的所有元素，使用foreach对每个元素进行处理
          it.foreach(id=>{ // TODO: warning 此处foreach不可以换成map，因为foreach的函数会立马执行，map里面的函数不会立马执行
            // 赋值
            statement.setInt(1, id)
            // 执行SQL
            val res: ResultSet = statement.executeQuery()
            while (res.next()) {
              name = res.getString("name")
              age = res.getInt("age")
            }
            result.+=((id, name, age))  // 在原集合添加元素
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (statement != null)
            statement.close()
          if (connection != null)
            connection.close()
        }
        println(s"${Thread.currentThread().getName} -- -- map ")
        result.toIterator
    })

    println(rdd2.collect().toList)

  }

  /**
   * mapPartitionsWithIndex(func:(Int, Iterator[RDD元素类型])=>Iterator[B])：一对一转换，原RDD一个分区计算得到新RDD一个分区
   *    里面的函数func是针对每个分区操作，分区有多少个，函数执行多少次
   *     函数第一个参数代表分区号
   *     函数第二个参数代表分区数据迭代器
   *    使用场景：一般用于从MySQL/hbase/redis查询数据，可以减少连接创建与销毁
   */
  @Test
  def mapPartitionsWithIndex(): Unit = {
    val rdd = sc.parallelize(List(1, 10, 3,4,5,6,7,8,9), 2)

    val rdd2 = rdd.mapPartitionsWithIndex((index, it) => {
      println(s"index=${index} -- data=${it.toList}")
      println(it)
      it
    })

    println(rdd2.collect().toList) // List()空的List，因为迭代器Iterator只能使用一次
  }

  /**
   * groupBy(func:RDD元素类型=>K)：按照指定字段分组
   *    func函数针对每个元素操作，元素个数=func函数执行次数
   *    根据函数的返回值对元素进行分组
   *    groupBy返回的RDD是KV键值对，K是函数的返回值，V是原RDD中K对应的所有元素的集合
   * Spark的groupBy会产生shuffle操作，所以有分区器
   *
   * MR执行流程：数据->InputFormat[分片]->反序列化操作->map(k,v)->
   *           环形缓冲区[内存区域，进行分组、分区、排序，80%的阈值，超过溢写到磁盘]->[Combiner操作]->磁盘->
   *           合并小文件->reducer拉数据->归并排序
   *           ->reduce(k,v)->outputFormat->磁盘
   * MR shuffle阶段：map方法之后，reduce方法之前，->环形缓冲区[内存区域，进行排序，80%的阈值，超过溢写到磁盘]->[Combiner操作]->磁盘->合并小文件->reducer拉数据->归并排序->
   *
   * Spark shuffle: ->缓冲区[32条且内存不够，超过溢写到磁盘，分区，不一定有排序]->[Combiner操作]->磁盘->合并小文件->子RDD拉数据->[归并排序]->
   *
   */
  @Test
  def groupBy(): Unit = {
    val rdd = sc.parallelize(List( ("1", "M", "BJ"), ("2", "M", "SH"), ("3", "W", "SZ") ), 3)
    val rdd2 = rdd.groupBy(x => x._2)
    println(rdd2.collect().toList)
  }

  /**
   * distinct: 去重
   * distinct 会产生shuffle操作，因为需要全局比较去重
   *
   */
  @Test
  def distinct(): Unit = {
    val rdd = sc.parallelize(List(1,4,3,2,11,11,11))

    // 去重API
    //val rdd2 = rdd.distinct()
    //println(rdd2.collect().toList)

    // 自行写去重函数
    def selfDistinct(list:RDD[Int]): List[Int] = {
      // 先groupBy，再map取key
      val tmpGroupBy: RDD[(Int, Iterable[Int])] = list.groupBy(x => x)
      val res: RDD[Int] = tmpGroupBy.map(x => x._1)
      res.collect().toList
    }

    val rdd2 = selfDistinct(rdd)
    println(rdd2)

  }

  /**
   * coalesce(分区数)：合并分区，将原rdd分区合并为指定数量
   *    默认只能减少分区数，此时没有shuffle操作
   *    如果想要增大分区数，需要设置shuffle=tru，此时会产生shuffle操作
   *    使用场景：一般用于减少分区数，一般用于搭配filter使用，filter会减少数据量，当一个分区数据量过滤之后变小了，单独起task划不来，不如合并
   *
   * 分区合并规则
   */
  @Test
  def coalesce(): Unit = {
//    val rdd = sc.parallelize(List(1,4,3,2,11,11,11,12,13,14,15,16), 6)
//    val rdd = sc.parallelize(List(1,2,3,4,5,6), 6)
    val rdd = sc.parallelize(List(0,1,2,3,4,5), 6)
    rdd.mapPartitionsWithIndex((index, it)=>{
      println(s"index:${index} -- data=${it.toList}")
      it
    }).collect()

    println("------------after coalesce------------")

//    val rdd2 = rdd.coalesce(4)
//    val rdd2 = rdd.coalesce(8)
    val rdd2 = rdd.coalesce(3, shuffle = true)
    println(rdd2.getNumPartitions)

    val rdd3 = rdd2.mapPartitionsWithIndex((index, it) => {
      println(s"index:${index} -- data=${it.toList}")
      it
    })

    rdd3.collect()
    // Thread.sleep(10000000)
  }

  /**
   * repartition: 重分区
   *    即可以增大分区数，也可以减少分区数，但是都有shuffle操作
   *    底层就是调用coalesce(numPartitions, shuffle = true)
   *
   * coalesce 与 repartition区别
   *    coalesce 默认只能减少分区数，此时没有shuffle操作
   *    repartition即可以增大分区，也可以减少分区数，但是都有shuffle操作
   *
   * repartition使用场景：增大分区数的时候，使用与在算子计算过程中数据量膨胀、增加的情况，这个时候增大分区可以提高性能，减少计算时间
   */
  @Test
  def repartition(): Unit = {

    val rdd = sc.parallelize(List(0,1,2,3,4,5), 6)
    val rdd2 = rdd.repartition(3)
    println(rdd2.getNumPartitions)

    val rdd3 = rdd.repartition(8)
    println(rdd3.getNumPartitions)
  }

  /**
   * sortBy(func:RDD元素类型=>K): 根据指定字段排序
   *    sortBy里面的函数func，针对每个元素进行操作，元素数量=函数执行次数
   *    根据函数的返回值对RDD元素进行重新排序
   */
  @Test
  def sortBy(): Unit = {
    val rdd = sc.parallelize(List(10,0,1,2,3,4,5), 6)

    val rdd2 = rdd.sortBy(x => x)

    println(rdd2.collect().toList)

  }

}