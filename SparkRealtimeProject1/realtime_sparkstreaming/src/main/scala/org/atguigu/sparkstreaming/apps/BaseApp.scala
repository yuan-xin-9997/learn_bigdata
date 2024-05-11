package org.atguigu.sparkstreaming.apps

import org.apache.spark.streaming.StreamingContext

import scala.util.control.Breaks

/**
 * @author: yuan.xin
 * @createTime: 2024/05/11 15:25
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 *
 * BaseApp: 编写一个SparkStreamingApp的一般步骤抽象出来。
 *  不同的业务类，只需要集成BaseApp，就可以拥有编程的一般步骤，无需重复编写
 *  BaseApp 自己没有实例化的意义，给子类继承使用
 */
abstract class BaseApp {

  // 只声明，未赋值，抽象属性，只有抽象类，才能声明抽象属性
  var groupId:String
  var topic:String
  var appName:String
  var batchDuration:Int   // seconds

  // 由子类去overwrite（覆写，子类可以根据自己的appName，自己的batchDuration，创建自己的StreamingContext
  var context:StreamingContext = null

  def runApp(code: => Unit) :Unit={
     try {
       code
       context.start()
       context.awaitTermination()
     } catch {
       case ex: Exception =>
         ex.printStackTrace()
         throw new RuntimeException("运行出错!")
     }
   }
}
