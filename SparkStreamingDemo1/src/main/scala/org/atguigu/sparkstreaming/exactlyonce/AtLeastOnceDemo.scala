package org.atguigu.sparkstreaming.exactlyonce

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* at most once 最少消费一次
* 丢失数据的原因：在输出之前，已经提交offset，一旦出现故障，只会从上次提交的位置往后消费，造成漏消费数据
* 解决方案：
*   1. 取消自动提交offset，改为手动提交
*   2. 在消费成功之后，再提交
* --------------------------------
* 写的代码，哪些在Driver端运行，哪些在Executor端运行
* java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute
* ----------------------------------
* OffsetRange(topic: 'topicA', partition: 0, range: [86 -> 86])
*   一个OffsetRange代表消费的一个分区一个主体，当前批次拉取到的起始和终止offset
*   关注 val untilOffset: Long
* --------------------------------------------
* local[*] ：本地模式以多线程模你分布式计算。
*   Executor的线程统一命名为：Executor task launch worker for task id
*   只要不是这个线程名，都是在Driver端运行
* -------------------------------------------
* 如何区分代码在Driver端还是在Executor端运行？
*   1. 如果是DStream普通算子（RDD中也有的同名算子），如map、filter等都是在Executor端运行
*   2 特殊算子
*     transform和foreachRDD 只有RDD.算子(xxx)   xxx是在Executor端运行，其余都是Driver端运行
* ---------------------------------------------
* 结论：偏移量offset是在Driver端获取，在Driver端提交
* */
object AtLeastOnceDemo {
  def main(args: Array[String]): Unit = {
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
      "group.id" -> "20240506",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),  // 自动提交
//      "auto.commit.interval.ms"->"500" // 自动提交的时间间隔，每间隔多久提交一次
    )

    // 要消费的主题：理论上允许消费多种主题的数据。但是一般操作时，只写一个主题
    //      原因：不同主题保存的数据类型是不一致的。如果一个流消费了两种不同的数据，流中混杂了两种数据，
//               在进行处理时，都需要对数据类型判断，判断是否需要处理的某种类型，编程逻辑难以维护，杂乱
//          如果需要消费两个主题，应该每个主题一个流，获取两个流，每个流中只有一种数据！
    val topics = Array("topicA")

    // 如何从Kafka数据源获取DStream
    //     全是固定代码
    /*
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
    * ConsumerRecord[K, V] 从kafka消费到的一条数据，一般只获取V
    * ProduceRecord[K, V] V封装data，K封装meta data（比如partition=0），主要用于分区等
    * */
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // 获取偏移量offset
    var ranges: Array[OffsetRange] = null  // 在Driver端
    println("a:"+Thread.currentThread().getName())
    val ds1: DStream[ConsumerRecord[String, String]] = ds.transform(rdd => { // 在Driver端 JobGenerator
      println("b:"+Thread.currentThread().getName())
      // 偏移量
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      for (elem <- ranges) {
//        println(elem)
//      }
      ranges.foreach(
        println(_)
      )
      rdd
    })

//    // 只取V
//    val ds1: DStream[String] = ds.map(record => record.value())
//
//    // 分割+压平，ds2中的一个string是一个单词
//    val ds2: DStream[String] = ds1.flatMap(line => line.split(" "))
//
//    // word count
//    val ds3: DStream[(String, Int)] = ds2.map(word => (word, 1)).reduceByKey(_ + _)

    // 输出：在屏幕打印
    //   print() 默认打印10行
    ds1.map(record=>{
      println("c:"+Thread.currentThread().getName())
      Thread.sleep(500)
      if(record.value().equals("B")){
        throw new RuntimeException("程序异常")
      }
      record.value()
    }).foreachRDD(rdd=>{
      println("d:"+Thread.currentThread().getName())
      // 输出
      rdd.foreach(word=>println("e:"+Thread.currentThread().getName() + ":" + word))   //rdd.xxx 里面的在Executor端执行

      // 手动提交offset
      ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
    })//.print(1000)

    // 不能直接暴露在Driver的main方法中，而应该在输出Operation中输出之后调用
    // 手动提交offset
    // 在Driver端运行
    //ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)

    // 启动APP
    streamingContext.start()

    // 阻塞进程，让进程一直运行
    streamingContext.awaitTermination()

  }
}
