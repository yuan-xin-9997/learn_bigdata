package org.atguigu.sparkstreaming.utils

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Date

/**
 * @author: yuan.xin
 * @createTime: 2024/05/13 19:43
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 */
object DataParseUtil {

  // 日期格式
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val dateTimeFormatterWithoutSeconds: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val timeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
  val hourFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")

  def parseMillTsToDateTime(MillTs:Long, format:DateTimeFormatter=null):String={
    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(MillTs), ZoneId.of("Asia/Shanghai"))
     if(format!=null){
       dateTime.format(format)
     }else{
       dateTime.format(dateTimeFormatter)
     }
  }

  def parseMillTsToDateTimeWithoutSeconds(MillTs:Long, format:DateTimeFormatter=null):String={
    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(MillTs), ZoneId.of("Asia/Shanghai"))
     if(format!=null){
       dateTime.format(format)
     }else{
       dateTime.format(dateTimeFormatterWithoutSeconds)
     }
  }

  def parseMillTsToDate(MillTs:Long, format:DateTimeFormatter=null):String={
    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(MillTs), ZoneId.of("Asia/Shanghai"))
     if(format!=null){
       dateTime.format(format)
     }else{
       dateTime.format(dateFormatter)
     }
  }

  def parseDateTimeStrToDate(dateTimeStr:String, format:DateTimeFormatter=null):String={
       if(format!=null){
         val dateTime: LocalDateTime = LocalDateTime.parse(dateTimeStr, format)
         dateTime.format(format)
       }else{
         val dateTime: LocalDateTime = LocalDateTime.parse(dateTimeStr, dateTimeFormatter)
         dateTime.format(dateFormatter)
       }
  }

  def parseDateTimeStrToHour(dateTimeStr:String, format:DateTimeFormatter=null):String={
       if(format!=null){
         val dateTime: LocalDateTime = LocalDateTime.parse(dateTimeStr, format)
         dateTime.format(format)
       }else{
         val dateTime: LocalDateTime = LocalDateTime.parse(dateTimeStr, dateTimeFormatter)
         dateTime.format(hourFormatter)
       }
  }

  def main(args: Array[String]): Unit = {
    val ts = 1715405686000l
    // Date对象
    val date = new Date(ts)
    // 日期格式
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    // 使用日期格式去格式化Date
    val datestr: String = simpleDateFormat.format(date)
    println(datestr)
    println("--------------------------java8之前------------------------")
    println("--------------------------java8之后------------------------")
    // 毫秒时间戳转换为时间格式字符串
    // java.time   静态方法
    // 日期格式
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    // LocalDate日期  LocalDateTime日期时间
    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"))
    // 使用日期格式化Date
    val dateTimeStr: String = dateTime.format(formatter)
    println(dateTimeStr)

    println("--------------------------java8之后: 时间日期的转换------------------------")
    // 时间格式字符串转换为
    val datetimestr = "2023-12-17 17:23:38"
    val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
    val formatter3: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")
    val dateTime1: LocalDateTime = LocalDateTime.parse(dateTimeStr, formatter)
    println(dateTime1.format(formatter1))
    println(dateTime1.format(formatter2))
    println(dateTime1.format(formatter3))

  }
}
