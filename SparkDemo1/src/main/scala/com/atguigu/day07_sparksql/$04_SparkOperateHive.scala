package com.atguigu.day07_sparksql

import com.atguigu.day07_sparksql.$01_UserDefinedUDAF.spark

object $04_SparkOperateHive {
  /**
   * todo 在IDEA中通过Spark操作（读写）Hive数据库中数据【相当于 Spark on Hive的模式  】:
   *    1. 引入spark-hive、mysql_jdbc依赖
   *    2. 将hive-site.xml放入resources目录（hive-site.xml中指定了Hive元数据信息存放位置）
   *    3. 在创建SparkSession的时候开启Hive支持
   *    4. 操作Hive
   *
   * Hadoop->Hive->Spark
   *    HDFS上的数据其实都是文件，没有良好的组织形式。
   *    Hive将这些数据规整起来，让用户可以以结构化数据库的角度去使用这些数据（建立了HDFS数据的元数据信息），在Hive上写HQL操作表翻译成了MapReduce去读HDFS的数据进行计算。
   *    由此用户操作HIve的时候就和操作一个MySQL数据库一样没什么差别。所以即可以说Hive是一个计算引擎，也可以理解为一个数据（仓）库。既然他是一个数据库，而Spark天然支持各种数据源的加载和运算，读Hive的数据和读MySQL的数据这个操作本身对用户来说并没有什么差别。所以Hive可以作为一种数据源在系统中存在，Spark、Flink、Java程序都可以连接访问，只是它还自带了执行引擎HQL。
   *    spark是一个通用的处理大规模数据的分析引擎，即 spark 是一个计算引擎，而不是存储引擎，其本身并不负责数据存储。其分析处理数据的方式，可以使用sql，也可以使用java,scala, python甚至R等api；其分析处理数据的模式，既可以是批处理，也可以是流处理；而其分析处理的数据，可以通过插件的形式对接很多数据源，既可以是结构化的数据，也可以是半结构化甚至分结构化的数据，包括关系型数据库RDBMS，各种nosql数据库如hbase, mongodb, es等，也包括文件系统hdfs，对象存储oss, s3 等等。
   *    “Apache Hive data warehouse software facilitates reading, writing, and managing large datasets residing in distributed storage using SQL.”，hive的定位是数据仓库，其提供了通过 sql 读写和管理分布式存储中的大规模的数据，即 hive即负责数据的存储和管理（其实依赖的是底层的hdfs文件系统或s3等对象存储系统），也负责通过 sql来处理和分析数据。所以说，hive只用来处理结构化数据，且只提供了sql的方式来进行分析处理。
   *    hive on spark。在这种模式下，数据是以table的形式存储在hive中的，用户处理和分析数据，使用的是hive语法规范的 hql (hive sql)。 但这些hql，在用户提交执行时，底层会经过解析编译以spark作业的形式来运行。（事实上，hive早期只支持一种底层计算引擎，即mapreduce，后期在spark 因其快速高效占领大量市场后，hive社区才主动拥抱spark，通过改造自身代码，支持了spark作为其底层计算引擎。目前hive支持了三种底层计算引擎，即mr, tez和spark.用户可以通过set hive.execution.engine=mr/tez/spark来指定具体使用哪个底层计算引擎。
   *    spark on hive。上文已经说到，spark本身只负责数据计算处理，并不负责数据存储。其计算处理的数据源，可以以插件的形式支持很多种数据源，这其中自然也包括hive。当我们使用spark来处理分析存储在hive中的数据时，这种模式就称为为 spark on hive. 这种模式下，用户可以使用spark的 java/scala/pyhon/r 等api，也可以使用spark语法规范的sql ，甚至也可以使用hive 语法规范的hql (之所以也能使用hql，是因为 spark 在推广面世之初，就主动拥抱了hive，通过改造自身代码提供了原生对hql包括hive udf的支持，这也是市场推广策略的一种吧）。
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    // todo: 指定操作Hadoop的用户，仅在Windows本地执行代码时候需要增加，打jar不需要
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
    import org.apache.spark.sql.{DataFrame, Row, SparkSession}
    val spark = SparkSession.builder()
        .appName("spark-sql")
        .master("local[4]")
        .enableHiveSupport()  // todo 开启Hive支持，可以读取Hive表信息，在sql()中可以直接写HQL，也可以完全使用SparkSQL的功能
        .config("spark.testing.memory", "2147480000")
        .getOrCreate()
    import spark.implicits._

    // todo 读取hive数据
    spark.sql("select * from emp").show()
    spark.sql("select * from score_info").show()
    spark.sql("select t.deptno, avg(t.sal) avg_sal from emp t group by t.deptno;").show()

    // todo 使用Spark的自定义UDAF函数读取Hive中的数据，并进行group by和求均值
    import org.apache.spark.sql.functions._
    spark.udf.register("myavg2", udaf(new StrongAvgUDAF))
    val res: DataFrame = spark.sql(
      """
        |select uid, myavg2(score) from score_info group by uid
        |""".stripMargin
    )
    res.show()

    // todo 写入数据到hive
    // load data ...
    // insert into ...
    // insert overwrite ...
    spark.sql("insert into table emp_20240116 select * from emp where sal > 10000")
  }
}
