package com.atguigu.day07_sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructType}

/**
 * todo 自定义弱类型UDAF函数Avg: 求平均值
 *    1. 创建Class继承抽象类UserDefinedAggregateFunction
 */
class WeakAvgUDAF extends UserDefinedAggregateFunction{
  /**
   * 指定UDAF函数的参数类型，[自定义avg函数，针对age列，Int类型]
   * @return
   */
  override def inputSchema: StructType = {
    new StructType()
      .add("input", IntegerType)
  }

  /**
   * 指定中间变量的类型【求一组区域的平均值，需要统计总年龄和人的个数】
   * @return
   */
  override def bufferSchema: StructType = {
    new StructType()
      .add("sum", IntegerType)
      .add("count", IntegerType)
  }

  /**
   * 指定UDAF最终计算结果类型
   * @return
   */
  override def dataType: DataType = DoubleType

  /**
   * 一致性的指定，是否同样的输入返回同样的输出
   * @return
   */
  override def deterministic: Boolean = true

  /**
   * 指定中间变量的初始值，sum=0, count=0
   * @param buffer
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // sum=0，顺序要和定义中间变量保持一致
    buffer(0) = 0
    // count=0，顺序要和定义中间变量保持一致
    buffer(1) = 0
  }

  /**
   * 类似预聚合combiner操作，针对每个分的组单个age值进行计算
   * @param buffer 中间变量的封装[sum,count]
   * @param input  组中的一个值[age]
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    println(s"update ${Thread.currentThread().getName} 中间变量:${buffer} 传入数据${input}")
    //sum = sum + age
    buffer(0) = buffer.getAs[Int](0) + input.getAs[Int](0)  // todo 取值必须用角标，不允许用列名
    //count = count + 1
    buffer(1) = buffer.getAs[Int](1) + 1
  }


  /**
   *
   * @param buffer1  中间变量的封装[sum, count]
   * @param buffer2   combiner的结果[sum, count]
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    println(s"merge ${Thread.currentThread().getName} 中间变量:${buffer1} combiner结果${buffer2}")
    // sum = sum + combiner_sum
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)  // todo 取值必须用角标，不允许用列名
    // count = count + combiner_count
    buffer1(1) = buffer1.getAs[Int](1) + buffer2.getAs[Int](1)
  }

  /**
   * 计算最终结果
   * @param buffer 中间变量的封装[sum, count]
   * @return
   */
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0).toDouble / buffer.getAs[Int](1).toDouble  // todo 输出类型是Double，此处需要手动转，spark不会自动转
  }
}