package com.atguigu.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.junit.{After, Before, Test}

class DSTest {

  //读取配置文件
  val conf: SparkConf= new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

  //创建sparkContext
  val sc = new SparkContext(conf)

  //创建sparkSession对象
  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换
  //spark不是包名，是上下文环境对象名
  import sparkSession.implicits._

  //必须是seq集合
  @Test
  def test1(): Unit = {

    val list = List(user2(1, 2), user2(1, 3), user2(4,5))

    /*val map = Map((1, 2), (1, 2), (1, 2))

    //非seq集合无法使用sparkSession
    val value: Any = sparkSession.sparkContext.makeRDD(map)

    val value1: Any = sc.makeRDD(map)*/

    //如果集合中是样例类，那么使用DF、DS都可以将列名打印出来
    val rdd2: RDD[user2] = sparkSession.sparkContext.makeRDD(list)
    rdd2.toDS().show()
    //show(2) 显示2行
    rdd2.toDF().show(2)
    //重命名列名
    rdd2.toDF("id2","salary2").show(2)

    sparkSession.stop()
    sc.stop()

  }

  @Test
  def test2(): Unit = {
    val list = List(user2(1, 2), user2(1, 3), user2(4,5))
    val rdd2: RDD[user2] = sparkSession.sparkContext.makeRDD(list)


    //RDD类型取单行数据
    rdd2.map(user => user.id).collect().foreach(println)

    //DF类型取出单行元素
    val df: DataFrame = rdd2.toDF()
    val rdd: RDD[Row] = df.rdd
    rdd.map{
      case a:Row => (a.getInt(0),a.getInt(1))
    }.collect().foreach(println)

    //DS类型取出单行元素
    val ds1: Dataset[user2] = df.as[user2]
    ds1.map(user => user.id).collect().foreach(println)


    sparkSession.stop()
    sc.stop()

  }





}
case class user2(id:Int,salary:Int)