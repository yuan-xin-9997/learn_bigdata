package org.atguigu.sparkstreaming.beans

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class CouponAlertInfo(id:String,  // 体现mid
                           uids:mutable.Set[String],
                           itemIds:mutable.Set[String],
                           events:ListBuffer[String],
                           ts:Long)