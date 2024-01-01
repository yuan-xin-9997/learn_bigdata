package com.atguigu.Test1.official

import scala.io.Source

object Test1 {

  /**
    * 1、获取没有农贸市场的省份
    * 2、统计菜的种类数最多的三个省份
    * 3、统计每个省份菜的种类数最多的三个农贸市场
    */

  def main(args: Array[String]): Unit = {

    //读取数据
    val allProvinces = Source.fromFile("datas/allprovince.txt").getLines().toList
    val products = Source.fromFile("datas/product.txt").getLines().toList

    //test1(products,allProvinces)

    //test2(products)

    test3(products)
  }

  /**
    * 3、统计每个省份菜的种类数最多的三个农贸市场
    */
  def test3(products:List[String]): Unit ={

    //1、是否需要过滤[过滤脏数据]
    val filterList = products.filter(line=> line.split("\t").size==6)
    //2、列裁剪[省份、农贸市场名称、菜名]
    val selectList = filterList.map(x=>{
        val arr = x.split("\t")

        (arr(4), arr(3), arr.head)
       })
    //List( (湖北省,A农贸市场,西蓝花 ), (湖北省,A农贸市场,西蓝花 ),(湖北省,B农贸市场,西蓝花 ) ,(湖北省,A农贸市场,大白菜 ),(湖南省,F农贸市场,鲫鱼 ),.....)

    //3、去重[同一个省份同一个农贸市场同一个菜只有一条数据]
    val disList = selectList.distinct
    //List( (湖北省,A农贸市场,西蓝花 ),(湖北省,B农贸市场,西蓝花 ) ,(湖北省,A农贸市场,大白菜 ),(湖南省,F农贸市场,鲫鱼 ),.....)

    //4、按照省份+农贸市场分组
    val groupedMap = disList.groupBy{
      case (province,market,num) => (province,market)
    }
    //Map(
    //   (湖北省,A农贸市场) -> List( (湖北省,A农贸市场,西蓝花 ), (湖北省,A农贸市场,大白菜 ),....)
    //   (湖北省,B农贸市场) -> List( (湖北省,B农贸市场,西蓝花 ),....)
    //    .....
    // )

    //5、统计每个省份每个农贸市场的菜种类数
    val provinceMarketNumList = groupedMap.map( x=>{
      //x =   (湖北省,A农贸市场) -> List( (湖北省,A农贸市场,西蓝花 ), (湖北省,A农贸市场,大白菜 ),....)
      (x._1, x._2.size)
    } )
    //Map( (湖北省,A农贸市场) ->15, (湖北省,B农贸市场) ->20 ,(湖南省,F农贸市场) ->34 ,.....)

    //6、按照省份分组
    val groupedProvinceMap = provinceMarketNumList.groupBy{
      case ( (province,market),num ) => province
    }
    //Map(
    //    湖北省 -> Map(   (湖北省,A农贸市场) ->15, (湖北省,B农贸市场) ->20  ,....)
    //    湖南省 -> Map(   (湖南省,F农贸市场) ->34 ,....)
    //    ...
    // )

    //7、对每个省份所有农贸市场数据按照菜的种类数排序取前三
    val result = groupedProvinceMap.map(x=>{
      //x =  湖北省 -> Map(   (湖北省,A农贸市场) ->15, (湖北省,B农贸市场) ->20  ,....)

      val top3 = x._2.toList.sortBy(y=> y._2 ).reverse.take(3)
      //去掉省份
      val t3 = top3.map{
        case ( (province,market) ,num ) => (market,num)
      }
      (x._1, t3)
    })

    //8、结果打印
    result.foreach(println)
  }

  /**
    * 2、统计菜的种类数最多的三个省份
    */
  def test2(products:List[String]): Unit ={

    //1、是否需要过滤[过滤脏数据]
    val filterList = products.filter(line=> line.split("\t").size ==6 )
    //2、是否需要列裁剪[菜名、省份]
    //List( (湖北省,西蓝花),(湖北省,西蓝花),(湖北省,大白菜),(湖北省,上海青),(湖南省,鲫鱼),..... )
    val selectList = filterList.map(line=>{
      val arr = line.split("\t")
      val province = arr(4)
      val name = arr.head
      ( province,name )
    })
    //3、去重
    val disList = selectList.distinct
    //List( (湖北省,西蓝花),(湖北省,大白菜),(湖北省,上海青),(湖南省,鲫鱼),..... )

    //4、按照省份分组
    val groupedMap = disList.groupBy{
      case (province,name) => province
    }
    //Map(
    //    湖北省 -> List( (湖北省,西蓝花),(湖北省,大白菜),(湖北省,上海青),... )
    //    湖南省 -> List( (湖南省,鲫鱼),... )
    //    ....
    // )

    //5、统计每个省份的菜的种类数
    val numList = groupedMap.map(x=>{
      //x =  湖北省 -> List( (湖北省,西蓝花),(湖北省,大白菜),(湖北省,上海青),... )
      ( x._1, x._2.size )
    })
    //Map(湖北省->23,湖南省->33,山西省->85,....)

    //6、按照菜的种类数排序取前三
    val result = numList.toList.sortBy(x=>x._2).reverse.take(3)
    //7、结果打印
    result.foreach(println)

  }


  /**
    * 1、获取没有农贸市场的省份
    */
  def test1(products:List[String],allProvinces:List[String]): Unit ={

    //1、是否需要过滤[过滤脏数据]
    val filterProducts = products.filter(line=> line.split("\t").size == 6)
    //2、是否需要列裁剪[省份]
    val productProvince = filterProducts.map(line => {
      val arr = line.split("\t")
      arr(4)
    })
    //3、是否需要去重[去重]
    val disProvinces = productProvince.distinct
    //4、全国所有省份与有农贸市场的省份求差集
    val result = allProvinces.diff(disProvinces)

    result.foreach(println)
  }
}
