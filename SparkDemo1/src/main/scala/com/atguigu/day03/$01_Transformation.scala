package com.atguigu.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.junit.Test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

class $01_Transformation{

  val conf = new SparkConf()
    .setMaster("local[4]").setAppName("test")
    .set("spark.testing.memory", "2147480000")
//    .set("spark.default.parallelism", "10")
  val sc = new SparkContext(conf)

  // Value类型才能使用的算子
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
   * Value类型才能使用的算子
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
   * Value类型才能使用的算子
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
   * Value类型才能使用的算子
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
   * Value类型才能使用的算子
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
   * Value类型才能使用的算子
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
   * Value类型才能使用的算子
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
   * Value类型才能使用的算子
   * sortBy(func:RDD元素类型=>K): 根据指定字段排序
   *    sortBy里面的函数func，针对每个元素进行操作，元素数量=函数执行次数
   *    根据函数的返回值对RDD元素进行重新排序
   *
   * 会产生shuffle，不可以使用HashPartitioner，无法计算出整体有序的结果。使用的是RangePartitioner，可保证整体有序
   */
  @Test
  def sortBy(): Unit = {
    val rdd = sc.parallelize(List(10,0,1,2,3,4,5), 6)

    val rdd2 = rdd.sortBy(x => x, ascending = false)

    println(rdd2.collect().toList)
  }

  /**
   * 双Value类型才能使用的算子
   * intersection：取两个RDD的交集
   *
   * 会产生2次shuffle，因为元素需要放到一起才能比较是否有没有重复.
   *
   * 会有3个stage，rdd1到rdd3、rdd2到rdd3都会有shuffle
   */
  @Test
  def intersection(): Unit = {
    val rdd1 = sc.parallelize(List(1,2,3,4,5))
    println(rdd1.getNumPartitions)
    val rdd2 = sc.parallelize(List(4,5,6,7,8))
    println(rdd2.getNumPartitions)

    // 此时rdd3依赖两个rdd
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.getNumPartitions)

    println(rdd3.collect().toList)

    Thread.sleep(10000000)
  }

  /**
   * 双Value类型才能使用的算子
   * subtract:差集
   * 和intersection一样会产生2次shuffle，使用HashPartitioner
   */
  @Test
  def subtract(): Unit = {
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    println(rdd1.getNumPartitions)
    val rdd2 = sc.parallelize(List(4, 5, 6, 7, 8))
    println(rdd2.getNumPartitions)
    // 此时rdd3依赖两个rdd
    val rdd3 = rdd1.subtract(rdd2)
    println(rdd3.getNumPartitions)
    println(rdd3.collect().toList)  // List(1, 2, 3)
    //Thread.sleep(10000000)
  }

  /**
   * 双Value类型才能使用的算子
   * union:并集不去重
   *
   * 由于只需要取并集，所以没有产生shuffle。
   * 并且rdd3的分区数=依赖的父rdd1+rdd2的分区数之和
   */
  @Test
  def union(): Unit = {
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    println(rdd1.getNumPartitions)
    val rdd2 = sc.parallelize(List(4, 5, 6, 7, 8))
    println(rdd2.getNumPartitions)
    // 此时rdd3依赖两个rdd
    val rdd3 = rdd1.union(rdd2)
    println(rdd3.getNumPartitions)
    println(rdd3.collect().toList) // List(1, 2, 3)
    //Thread.sleep(10000000)
  }

  /**
   * 双Value类型才能使用的算子
   * zip: 拉链
   *    要求：两个RDD的元素个数、分区个数都必须一致
   *
   * 由于会变为1个rdd因此会产生shuffle？
   */
  @Test
  def zip(): Unit = {
//    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5), 2)
    //val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6)) // 只能拉链元素个数相同的RDD，报错Can only zip RDDs with same number of elements in each partition
    println(rdd1.getNumPartitions)
    val rdd2 = sc.parallelize(List("4", "5", "6", "7", "8"), 4)  // 只能拉链分区数相同的RDD，报错java.lang.IllegalArgumentException: Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    println(rdd2.getNumPartitions)
    // 此时rdd3依赖两个rdd
    val rdd3 = rdd1.zip(rdd2)
    println(rdd3.getNumPartitions)
    println(rdd3.collect().toList) // List(1, 2, 3)
    //Thread.sleep(10000000)
  }

  /**
   * Key-Value类型才能使用的算子
   * partitionBy(partitioner: Partitioner): RDD[(K, V)]  按照K重新分区
   * 会有shuffle操作
   */
  @Test
  def partitionBy(): Unit = {
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    rdd1.mapPartitionsWithIndex((index, it) => {
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()

    //rdd1.partitionBy//
    val rdd2: RDD[(Int, Null)] = rdd1.map(x => (x, null))
    val rdd3 = rdd2.partitionBy(new HashPartitioner(5))  // 传分区对象

    rdd3.mapPartitionsWithIndex((index, it)=>{
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()
  }

  /**
   * Key-Value类型使用算子
   * 自定义分区器 partitionBy
   *
   * todo：所有有shuffle过程的算子，都可以传自定义分区器，也可以传分区数
   */
  @Test
  def userPartitioner(): Unit = {
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    rdd1.mapPartitionsWithIndex((index, it) => {
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()
    println("--------------------------------")

    val rdd2: RDD[(Int, Null)] = rdd1.map(x => (x, null))

    // 需求：将%2==0的数据放入0号分区，%3==0的放入1号分区，%4==0的放入2号分区，其他放入3号分区
    val rdd3 = rdd2.partitionBy(new UserDefinedPartitioner(6))

    rdd3.mapPartitionsWithIndex((index, it) => {
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()
  }

  /**
   * Key-Value类型算子
   * groupByKey：按照K重新分组
   *      生成的新RDD的元素类型是KV键值对
   *      K是分组的Key
   *      V是Key在原RDD中对应的所有的value值，todo 注意不是元素，要与groupBy区别开来
   * 注意和单value类型的算子groupBy的区别，他们都会有shuffle计算
   */
  @Test
  def groupByKey(): Unit = {
    val rdd = sc.parallelize(List( ("1", "BJ"), ("2", "SH"), ("3", "SH"), ("1", "GZ")))

    println(rdd.getNumPartitions)
    val rdd2 = rdd.groupByKey(3)
    println(rdd2.getNumPartitions)
    println(rdd2.collect().toList)  // List((3,CompactBuffer(SH)), (1,CompactBuffer(BJ, GZ)), (2,CompactBuffer(SH)))

    // 使用groupBy实现groupByKey的效果
    // todo spark中groupBy底层实现就是this.map(t => (cleanF(t), t)).groupByKey(p)
    println("使用groupBy实现groupByKey的效果")
    val rdd3 = rdd.groupBy(x => x._1)
    println(rdd3.collect().toList)  // List((1,CompactBuffer((1,BJ), (1,GZ))), (2,CompactBuffer((2,SH))), (3,CompactBuffer((3,SH))))

    val rdd4 = rdd3.map(x => (x._1, x._2.map(y => y._2)))
    println(rdd4.collect().toList)
  }

  /**
   * WordCount案例，作为与下面ReduceByKey实现WordCount案例的对比
   */
  @Test
  def WordCountNormal(): Unit = {
    // 读取数据
    val rdd1 = sc.textFile("datas/wc.txt")
    println(rdd1.getNumPartitions)

    // flatMap操作
    val rdd2 = rdd1.flatMap(line => line.split(" "))

    // groupBy操作
    val rdd3 = rdd2.groupBy(x => x)

    // map操作
    val rdd4 = rdd3.map(x => (x._1, x._2.size))

    // 输出结果
    println(rdd4.collect().toList)
  }

  /**
   * Key-Value类型算子
   * reduceByKey(func:(value值类型, value值类型)=>value值类型): 对Key分组，对Value值聚合
   *     里面的函数func 第一个参数：代表上一次聚合结果，第一次聚合时代表当前组第一个值
   *                  第二个参数：代表当前待聚合的value值
   *     有shuffle操作
   *     过程：缓冲区，按Key分组，预聚合，Combiner(Reduce)，落盘，reduce拉数据，分组，reduce计算
   *
   * reduceByKey 与 groupByKey的区别：
   *      reduceByKey有预聚合操作（Combiner，就是类似在Map阶段做Reduce），性能更高，工作中推荐使用这种高性能的shuffle算子
   *      groupByKey没有预聚合操作
   */
  @Test
  def reduceByKey(): Unit = {
    // 使用reduceByKey的WordCount案例
    val rdd1 = sc.textFile("datas/wc.txt")
    println(rdd1.getNumPartitions)
    val rdd2 = rdd1.flatMap(line => line.split(" "))

    val rdd3: RDD[(String, Int)] = rdd2.map(x => (x, 1))
    rdd3.mapPartitionsWithIndex((index, it)=>{
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()

    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((agg, cur) => {
      println(s"agg=${agg} cur=${cur}")
      agg + cur
    })
    println(rdd4.getNumPartitions)
    println(rdd4.collect().toList)
  }

  /**
   * 使用groupByKey 统计stu_score.txt中每门学科的平均分
   */
  @Test
  def Test(): Unit = {
    // 统计stu_score.txt中每门学科的平均分
    val rdd1 = sc.textFile("datas/stu_score.txt")

    val rdd2 = rdd1.map(line => {
      val arr = line.split(",")
      val xueke_name = arr(1)
      val score = arr(2).toInt
      (xueke_name,score)
    })
    println(rdd2.collect().toList)

    val rdd3 = rdd2.groupByKey()  // 按Key为学科进行分组
    println(rdd3.collect().toList)

    val rdd4 = rdd3.map(x => (x._1, x._2.sum.toDouble / x._2.size))
    println(rdd4.collect().toList)
  }

  /**
   * 使用reduceByKey 统计stu_score.txt中每门学科的平均分
   */
  @Test
  def Test2(): Unit = {
    // 统计stu_score.txt中每门学科的平均分
    val rdd1 = sc.textFile("datas/stu_score.txt")
    println(rdd1.getNumPartitions)

    val rdd2: RDD[(String, (Int, Int))] = rdd1.map(line => {
      val arr = line.split(",")
      val xueke_name = arr(1)
      val score = arr(2).toInt
      (xueke_name, (score, 1))
    })
    println(rdd2.getNumPartitions)
    rdd2.mapPartitionsWithIndex(
      (index, it) => {
        println(s"index=${index} data=${it.toList}")
        it
      }
    ).collect()
    println(s"rdd2 = ${rdd2.collect().toList}")

    val rdd4 = rdd2.reduceByKey((agg, curr) => {
      println(s"agg=${agg} -- curr=${curr}")
      (agg._1 + curr._1, agg._2 + curr._2)
    })
    println(s"rdd4=${rdd4.collect().toList}")

    val rdd5 = rdd4.map {
      case (name, (score, num)) => (name, score.toDouble / num)
    }
    println(s"rdd5=${rdd5.collect().toList}")

  }

}


/**
 * 自定义分区器
 * @param num 分区数量
 */
class UserDefinedPartitioner(num:Int) extends Partitioner{
  // 获取新RDD的分区数
  override def numPartitions: Int = num

  // 获取Key对应的分区号
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int] match {
      case x if(x%2==0)=>0
      case x if(x%3==0)=>1
      case x if(x%4==0)=>2
      case _=>3
    }
  }
}