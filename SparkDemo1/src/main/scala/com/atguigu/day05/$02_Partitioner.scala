package com.atguigu.day05

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.junit.Test

class $02_Partitioner {
  val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)
  /**
   * Spark分区器
   * Spark自带的分区器有两种：
   *    HashPartitioner
   *         分区规则：key.hashCod % 分区数 < 0  ?  (key.hashCode % 分区数) + 分区数  :    key.hashCode % 分区数
   *    RangePartitioner
   *          分区规则：
   *              1、先对RDD数据抽样，确定 分区数-1 个key
   *              2、使用获取的key确定每个分区的数据边界
   *              3、后续使用每个数据的key与分区边界对比，如果在边界界定的范围内，则将该数据放入当前分区中
   * @param args
   */
  @Test
  def rangePartitioner(): Unit = {
    val rdd = sc.parallelize(List(1, 4, 2, 5, 6, 7, 8, 3, 5, 7, 5,55,56))
    val rdd2 = rdd.map(x => (x, null))
    val rdd3 = rdd2.partitionBy(new RangePartitioner(5, rdd2))  // 分为5个分区

    // todo 抽样计算出的 rangeBounds = [3, 5, 6, 8]
    //    <=3的数据会存入分区index=0
    //    3< and <=5的数据会存入分区index=1
    //    5< and <=6的数据会存入分区index=2
    //    <6 and <=8的数据会存入分区index=3
    //    >8 的数据会存入分区index=4

    rdd3.mapPartitionsWithIndex((index, it)=>{
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()
  }
}
