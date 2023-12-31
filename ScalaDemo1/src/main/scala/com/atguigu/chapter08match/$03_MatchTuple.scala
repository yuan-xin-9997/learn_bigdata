package com.atguigu.chapter08match

object $03_MatchTuple {

  /**
   * 匹配元组
   * @param args
   */
  def main(args: Array[String]): Unit = {
     val t = ("lisi",20,"深圳")

    // 元组在匹配的时候,变量是几元元组,匹配条件就只能是几元元组
    t match {
      case (x, _, z) =>
        println(x)
        println(z)
      // case (x,y) // Pattern type is incompatible with expected type, found: (Any, Any), required: (String, Int, String)
    }

    val list4 = List(
      ("宝安区", ("宝安中学", ("王者峡谷班", ("安其拉", 18)))),
      ("宝安区", ("宝安中学", ("王者峡谷班", ("甄姬", 18)))),
      ("宝安区", ("宝安中学", ("王者峡谷班", ("妲己", 18)))),
      ("宝安区", ("宝安中学", ("王者峡谷班", ("王昭君", 18)))),
      ("宝安区", ("宝安中学", ("王者峡谷班", ("扁鹊", 18))))
    )

    val list5 = list4.map(

      x=>{
        x match {
          case (region, (schoolName, (_, (studentName, age)))) => studentName
        }
      }

    )



    print(list5)



  }
}
