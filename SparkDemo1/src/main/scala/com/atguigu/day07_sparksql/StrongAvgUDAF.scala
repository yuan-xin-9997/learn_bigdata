package com.atguigu.day07_sparksql

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
 * 自定义强类型UDAF函数：
 *    1. 定义class继承Aggregator[IN, BUFF, OUT]
 *      IN：代表UDAF函数的输入参数类型
 *      BUFF：代表计算过程中中间变量的类型
 *      OUT：最终计算结果类型
 *    2. 重写抽象方法
 *       注意：强类型UDAF需要重写的抽象方法少了几个，其中，底层deterministic一定为true，所以不需要重写，inputSchema、bufferSchema、dataType
 *            已经在泛型中定义，不需要重写;
 *            增加了两个针对中间结果、最终结果序列化的抽象方法
 *    3. 使用
 *       （1）创建自定义UDAF对象：val obj = new XXX
 *       （2）导入转换方法 import org.apache.spark.sql.function._
 *       （3）转换 val function = udaf(obj)
 *       （4）注册 spark.udf.register("函数名", function)
 */
class StrongAvgUDAF extends Aggregator[Int, AvgBuff, Double]{
  /**
   * 初始化中间变量值，功能与弱类型UDAF函数定义的抽象方法initialize等同
   * @return
   */
  override def zero: AvgBuff = AvgBuff(0, 0)

  /**
   * combiner计算函数，功能与弱类型UDAF函数定义的抽象方法update等同
   * @param buff 中间结果
   * @param age  udaf参数
   * @return 返回累加之后的中间结果
   */
  override def reduce(buff: AvgBuff, age: Int): AvgBuff = AvgBuff(buff.sum + age, buff.count + 1)

  /**
   * reduce聚合函数，功能与弱类型UDAF函数定义的抽象方法merge等同
   * @param buff1 中间结果
   * @param buff2 combiner聚合结果
   * @return 返回累加之后的中间结果
   */
  override def merge(buff1: AvgBuff, buff2: AvgBuff): AvgBuff = AvgBuff(buff1.sum + buff2.sum, buff1.count + buff2.count)

  /**
   * 计算最终结果，功能与弱类型UDAF函数定义的抽象方法evaluate等同
   * @param reduction
   * @return
   */
  override def finish(reduction: AvgBuff): Double = reduction.sum.toDouble / reduction.count

  /**
   * 指定中间结果的序列化类型
   * @return
   */
  override def bufferEncoder: Encoder[AvgBuff] = Encoders.product[AvgBuff]

  /**
   * 指定最终结果的序列化类型
   * @return
   */
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

case class AvgBuff(sum:Int, count:Int)