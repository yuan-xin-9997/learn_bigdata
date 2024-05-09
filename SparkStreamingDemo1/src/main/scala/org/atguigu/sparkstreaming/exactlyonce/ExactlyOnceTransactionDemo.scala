
package org.atguigu.sparkstreaming.exactlyonce

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.atguigu.sparkstreaming.utils.JDBCUtil

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util
import scala.collection.mutable

/*
* at least once
         +
* 将结果和offsets在一个事务中写出 = exactly once
*
* 数据库按照 特性 可分为 两种：
*   OLAP数据库：online analytic process 在线分析（大型查询）处理
*       hbase（filter）、es（DSL）、clickhouse、TIDB、Presto、Impala、SparkSQL
*       OLAP数据库基本都是NoSQL数据，且是分布式，分布式最难实现的就是事务，一般支持部分事务
*       hbase：如果要写入的数据都在1行，可以保证事务，但是不在1行，不可以 保证事务，即只能保证部分事务
*   OLTP数据库：online transaction process 在线事务处理
*       RDMS： mysql、oracle、DB2、sql server、Postgres SQL
* 数据库按照模式（是不是关系型数据库）分为：NoSQL 和 RDMS
*
* Hive是基于OLAP的数据仓库
* 数据仓库：历史版本留存
* 数据库：只保留最新版本
*
* Redis是NoSQL数据库，没有分析、事务功能
*
* GreenPlum是OLAP、OLTP？
*   Greenplum本质上是一个关系型数据库集群，实际上是由多个独立的数据库服务组合而成的一
*   个逻辑数据库。这种数据库集群采取的是MPP（Massively Parallel Processing）架构，大规模并行处理。
*   Greenplum 是全球领先的大数据分析引擎，专为数据分析、机器学习和人工智能而打造。
*   Greenplum是一个关系型数据库，是由数个独立的数据服务组合成的逻辑数据库，整个集群由多个数据节点（Segment Host）和控制节点
*   （Master Host）组成。在典型的Shared-Nothing中，每个节点上所有的资源的CPU、内存、磁盘都是独立的，每个节点都只有全部数据的
*  一部分，也只能使用本节点的数据资源。在Greenplum中，需要存储的数据在进入到表时，将先进行数据分布的处理工作，将一个表中的数据平
*   均分布到每个节点上，并为每个表指定一个分布列（Distribute Column）,
*   之后便根据Hash来分布数据，基于Shared-Nothing的原则，Greenplum这样处理可以充分发挥每个节点处IO的处理能力。
*   https://cn.greenplum.org/greenplum-6-0-from-olap-to-htap/
*
* GreenPlum可以理解为分布式关系型数据库集群，原生支持OLAP，也支持OLTP功能。基于大规模并行处理MPP架构，share-nothing架构
* HTAP 一词最早由 Gartner 于2014年左右提出。Forrester 也提出了类似的概念，称为 Translytical 。其主要思想是避免传统OLTP和OLAP分离的架构，而
* 采用混合 OLTP + OLAP 的架构。传统的架构如下图所示。OLTP 数据库支撑事务业务，OLAP 数据库支撑分析业务。
* Greenplum HTAP之路
作为一款主打 OLAP 和数据分析的数据库，过去十几年来 Greenplum 团队一直以分析型查询作为主要优化对象。近年来随着 Post
* greSQL 内核的升级（Greenplum 6.0 搭载 PostgreSQL 9.4 内核，Master 分支目前是 PostgreSQ
* L 9.6内核）和客户对 OLTP 型查询需求的提升， 6.0 开发周期中投入部分精力，对OLTP型查询进行了优化。总体看起来效果非常不错。
*
* 步骤：
* // The details depend on your data store, but the general idea looks like this

// begin from the offsets committed to the database  查询之前写入数据库的偏移量
val fromOffsets = selectOffsetsFromYourDatabase.map { resultSet =>
  new TopicPartition(resultSet.string("topic"), resultSet.int("partition")) -> resultSet.long("offset")
}.toMap

* // 从offsets位置获取一个流
val stream = KafkaUtils.createDirectStream[String, String](
  streamingContext,
  PreferConsistent,
  Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
)

stream.foreachRDD { rdd =>
  val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

  // 自己的运算得到结果
  val results = yourCalculation(rdd)

  // 开启事务
  // begin your transaction

  // update results
  // update offsets where the end of existing offsets matches the beginning of this batch of offsets
  // assert that offsets were updated correctly

  // end your transaction
  *
  *
}
*
* 以WordCount（累加）为例：
*   设计mysql表中怎么存储数据，offsets
*   数据：
*     粒度：一个word是一行
*     主键：word
*   offset：
*     groupID、topic、partitionID、offset
*     粒度：一个消费者组消费一个主题的一个分区是一行
*     主键：联合主键(groupID、topic、partitionID)
* */
object ExactlyOnceTransactionDemo {

  val groupId = "20240506"
  val topic = "topicA"

  /*
  *
  * 查询MySQL中已经提交的偏移量，根据当前组所消费的topic去查询
  * */
  def selectOffsetsFromMysql(groupId:String, topic:String):Map[TopicPartition, Long]={
    var connection: Connection = null
    var ps: PreparedStatement = null
    val sql =
      """
        |select
        | partitionId,
        | offset
        |from offsets
        |where groupId=? and topic=?
        |""".stripMargin
    var offsets = new mutable.HashMap[TopicPartition, Long]()
    try {
      connection = JDBCUtil.getConnection()
      ps = connection.prepareStatement(sql)
      ps.setString(1,groupId)
      ps.setString(2,topic)
      val resultSet: ResultSet = ps.executeQuery()
      while (resultSet.next()){
        offsets.put(new TopicPartition(topic, resultSet.getInt("partitionId")), resultSet.getLong("offset"))
      }
    } catch {
      case e:Exception =>{
        e.printStackTrace()
        throw new RuntimeException("查询偏移量失败")
      }
    } finally {
      if (ps != null){
        ps.close()
      }
      if (connection != null){
        connection.close()
      }
    }
    // 可变转不可变
    offsets.toMap
  }

  private def writeResultAndOffsetsInCommonTransaction(result: Array[(String, Int)], ranges: Array[OffsetRange]): Unit = {
    // 写单词：累加，将当前单词在数据库中已存在的部分和当前批次计算的相同的单词的值累加
    val sql1 =
      """
        |insert into wordcount values (?, ?)
        |on duplicate key update count=count+values(count)
        |""".stripMargin
    // 写偏移量
    val sql2 =
      """
        |insert into offsets values (?, ?, ?, ?)
        |on duplicate key update offset=values(offset)
        |""".stripMargin
//    val sql3 =
//      """
//        |replace into offsets values (?, ?, ?, ?)
//        |""".stripMargin

    var connection: Connection = null
    var ps1: PreparedStatement = null
    var ps2: PreparedStatement = null
    try {
      connection = JDBCUtil.getConnection()
      // 开启事务（即取消事务自动提交，改为手动提交
      connection.setAutoCommit(false)
      ps1 = connection.prepareStatement(sql1)
      ps2 = connection.prepareStatement(sql2)
      /*
      * result:  [(a,1),(b,2),....]
      * 每遍历一个单词，生成一条sql语句
      *   insert into wordcount values ('a', 1) on duplicate key update count=count+values(count)
      *   insert into wordcount values ('b', 2) on duplicate key update count=count+values(count)
      *   insert into wordcount values ('c', 3) on duplicate key update count=count+values(count)
      * */
      for ((word, count) <- result) {
        ps1.setString(1,word)
        ps1.setLong(2,count)
        // 将sql攒起来
        ps1.addBatch()
      }
      for (offSetRange <- ranges) {
        ps2.setString(1,groupId)
        ps2.setString(2,offSetRange.topic)
        ps2.setInt(3,offSetRange.partition)
        ps2.setLong(4,offSetRange.untilOffset)
        ps2.addBatch()
      }
      val res1: Array[Int] = ps1.executeBatch()
      val res2: Array[Int] = ps2.executeBatch()
      // 手动提交事务
      connection.commit()
      println("数据写入："+res1.size)
      println("偏移量写入："+res2.size)
    } catch {
      case e:Exception =>{
        // 回滚事务
        connection.rollback()
        e.printStackTrace()
        throw new RuntimeException("查询偏移量失败")
      }
    } finally {
      if (ps1 != null){
        ps1.close()
      }
      if (ps2 != null){
        ps1.close()
      }
      if (connection != null){
        connection.close()
      }
    }
  }


  def main(args: Array[String]): Unit = {

    // 查询MySQL偏移量
    val offsetsMap: Map[TopicPartition, Long] = selectOffsetsFromMysql(groupId, topic)


    // 创建 streamingContext 方式1
    // Create a StreamingContext by providing the details necessary for creating a new SparkContext.
    //Params:
    //master – cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
    //appName – a name for your job, to display on the cluster web UI
    //batchDuration – the time interval at which streaming data will be divided into batches
    //  batctDuration: Duration  一个批次的持续时间，采集多久的数据为1个批次
    //                          Milliseconds(毫秒数)
    //                          Seconds(秒数)
    //                          Minutes(分钟数)
//    val streamingContext = new StreamingContext("local[*]", "WordCountDemo", Seconds(10))

    // 创建 streamingContext 方式2
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountDemo")
//    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    // 创建 streamingContext 方式3
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountDemo").set("spark.testing.memory", "2147480000")
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))

    // 如果想获取其中的SparkContext
//    val sparkContext1 = streamingContext.sparkContext


    // 2. 从StreamingContext中获取DStream
    //    参考不同的数据源，获取不同的DStream。hdfs、kafka、TCP Socket
    //
//            streamingContext.fileStream()  流式读取HDFS目录中新增的文件
//            streamingContext.socketStream()  流失读取固定主机:port下发送的数据

    /*
    * App扮演的是消费者的角色。只要是消费者都要设置参数
    *   必须有：
    *   1. bootstrap.servers
    *   2. key/value的反序列化器
    *   3. group.id
    *
    *   auto.offset.reset: 从主题的哪个位置开时消费
    *     earliest: 如果group从来没有消费过主题，从主题的最早位置开时消费
    *     latest: 如果group从来没有消费过主题，从主题的最后（当前、最新）位置开时消费
    *     none: 如果组已经消费了，从earliest转为none，从已经提交的offsets后继续消费
    *   enable.auto.commit: 是否允许consumer自动提交offsets
    *     如果使用的是kafka 0.10版本以上的版本，都是将offsets提交到kafka内置的 _consumer_offsets中
    * */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),  // 自动提交，改为false=at least once
//      "auto.commit.interval.ms"->"500" // 自动提交的时间间隔，每间隔多久提交一次
    )

    // 要消费的主题：理论上允许消费多种主题的数据。但是一般操作时，只写一个主题
    //      原因：不同主题保存的数据类型是不一致的。如果一个流消费了两种不同的数据，流中混杂了两种数据，
//               在进行处理时，都需要对数据类型判断，判断是否需要处理的某种类型，编程逻辑难以维护，杂乱
//          如果需要消费两个主题，应该每个主题一个流，获取两个流，每个流中只有一种数据！
    val topics = Array(topic)

    // 如何从Kafka数据源获取DStream
    //     全是固定代码
    /* 函数签名
    * def createDirectStream[K, V](
          ssc: StreamingContext,    程序入口
          locationStrategy: LocationStrategy,  位置策略
          *               kafka的broker和SparkApp的Executor的位置关系（是不是同一个机器，同一个机架，同一个机房）
          *               调度Task到Executor时，有本地化（移动计算，而不是移动数据）级别。
          *               如果当前要消费的TopicA的0号分区的Leader在Hadoop102机器
          *               App恰好在Hadoop102启动了一个Executor，那么这个Task就应该调度给102的Executor，不应该给其他的Executor
          *
          *               99%都是PreferConsistent
          consumerStrategy: ConsumerStrategy[K, V]   消费策略：
          *               独立消费者：明确告诉要消费哪个主题的哪个分区，从哪个offset开时消费
          *                       创建Assign类
          *               非独立消费者：明确告诉消费哪个主题。由Kafka集群自动给消费者组中的每个线程分配分区，读取之前提交的offsets
          *                       Subscribe 类
          *                       此之前提交的位置去消费
        ): InputDStream[ConsumerRecord[K, V]] = {
        val ppc = new DefaultPerPartitionConfig(ssc.sparkContext.getConf)
        createDirectStream[K, V](ssc, locationStrategy, consumerStrategy, ppc)
    *
    *  ConsumerRecord[K, V] 从kafka消费到的一条数据，一般只获取V
    * ProduceRecord[K, V] V封装data，K封装meta data（比如partition=0），主要用于分区等
    * */
    // ds: DirectKafkaInputDStream 会每10s采集到的数据封装为KafkaRDD
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams, offsetsMap)  // 从查询的偏移量位置往后消费
    )

    // 业务处理
    ds.foreachRDD(
      rdd=>{
        if(!rdd.isEmpty()){
          // 获取当前偏移量 Driver端运行（注：即便kafka没有新数据产生，此处也会获取到偏移量
          val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          // 转换运算，分布式运算，在Executor端
          val result: Array[(String, Int)] = rdd.flatMap(record => record.value().split(" "))
            .map(word => (word, 1))
            .reduceByKey(_ + _)
            .collect()  // 收集计算结构

          // 将result和ranges在一个事务中写出
          writeResultAndOffsetsInCommonTransaction(result, ranges)
        }
      }
    )

    // 启动APP
    streamingContext.start()

    // 阻塞进程，让进程一直运行
    streamingContext.awaitTermination()

  }

}
