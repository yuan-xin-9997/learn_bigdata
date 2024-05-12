package org.atguigu.sparkstreaming.apps

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.gson.Gson
import org.junit.Test

import java.util
import scala.collection.mutable.ListBuffer

/**
 * @author: yuan.xin
 * @createTime: 2024/05/12 11:53
 * @contact: yuanxin9997@qq.com
 * @description: ${description}
 *
 *               fastjson:快，处理1000w条json，耗时短，但不稳定
 *               jsonstr:
 *               {} json对象 单个
 *               [] json数组 多个
 *
 *               对象转jsonstr（推荐使用gson）： JSON.toJSONString(对象|集合)
 *               jsonstr转对象：JSON.parseXxx()   {}转单个对象 JSON.parseObject('{}')   []转集合对象 JSON.parseArray("[]")
 *               gson: google json 国外都用，内置
 *               fastjson全部都是静态方法
 *               gson全部都是实例方法
 *
 * ---------------------------------------------------------------------------
 * 总结：
 *
 */
class JSONTest {
  @Test
  def testCase(): Unit = {
    val person: Person = Person("marry", 40)
    //    val persons: ListBuffer[(String, Int)] = ListBuffer(("jack", 20), ("tom", 30))

    val persons = new util.ArrayList[Person]()
    persons.add(person)

    /*ambiguous reference to overloaded definition,
both method toJSONString in class JSON of type (x$1: Any, x$2: com.alibaba.fastjson.serializer.SerializerFeature*)String
and  method toJSONString in class JSON of type (x$1: Any)String
match argument types (org.atguigu.sparkstreaming.apps.Person) and expected result type String
    val str: String = JSON.toJSONString(person)
    scala的case class才会出现
    */
    //    val str: String = JSON.toJSONString(person)
    //    println(str)

    val gson = new Gson()
    println(gson.toJson(person))
    println(gson.toJson(persons))
  }

  @Test
  def testCase2(): Unit = {
    val str =
      """
        |{"name":"marry","age":40}
        |""".stripMargin

    val person: Person = JSON.parseObject(str, classOf[Person])
    println(person)

    // 同样也是Map
    // public class JSONObject extends JSON implements Map<String, Object>, Cloneable, Serializable, InvocationHandler
    val jSONObject: JSONObject = JSON.parseObject(str)
    println(jSONObject.getString("name"))
    println(jSONObject.getString("age"))
  }

  @Test
  def testCase3(): Unit = {
    val str =
      """
        |[{"name":"marry","age":40}]
        |""".stripMargin

    val persons: util.List[Person] = JSON.parseArray(str, classOf[Person])
    println(persons)

    // public class JSONArray extends JSON implements List<Object>, Cloneable, RandomAccess, Serializable {
    val jSONArray: JSONArray = JSON.parseArray(str)
    println(jSONArray.get(0))

  }

}

case class Person(name: String, age: Int)