import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
* 编程入口（上下文Context）和编程模型
* 编程入口：
*   SparkCore：SparkContext
*   SparkSQL：SparkSession（内置SparkContext）
*   SparkStreaming：StreamingContext（内置SparkContext）
*
* 编程模型：
*   SparkCore：RDD
*   SparkSQL：DataFrame、DataSet
*   SparkStreaming：DStream
*
* StreamingContext：编程的核心入口。
*   用来从多种数据源创建
*   使用步骤：
*     1.创建StreamingContext
*     2. 从StreamingContext中获取DStream
*     3. 调用DStream的算子（高度抽象原语）计算
*     4. 以上3步都是懒加载，什么时候开始真正运算
*       启动App
*       StreamingContext.start()
*       停止
*       StreamingContext.stop()
*       流式计算：
*         启动后，一定24h不停运算
*         StreamingContext.awaitTermination() 等待发停止信号 或出现异常终止
*
* --------------------------------
* 参考官网：https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
* */
object WordCountDemo1 {
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
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountDemo")
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(10))

    // 如果想获取其中的SparkContext
//    val sparkContext1 = streamingContext.sparkContext


    // 2. 从StreamingContext中获取DStream
    //    参考不同的数据源，获取不同的DStream。hdfs、kafka、TCP Socket
    //
//            streamingContext.fileStream()  流式读取HDFS目录中新增的文件
//            streamingContext.socketStream()  流失读取固定主机:port下发送的数据


    // 启动APP
    streamingContext.start()

    // 阻塞进程，让进程一直运行
    streamingContext.awaitTermination()

  }
}
