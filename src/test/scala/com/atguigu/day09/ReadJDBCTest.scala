package com.atguigu.day09

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.Test

class ReadJDBCTest{

  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  import sparkSession.implicits._


  //读取wind数据库
  @Test
  def test1(): Unit = {
     //通用方法读取
    sparkSession.read.format("jdbc").
      option("url","jdbc:mysql://localhost:3306/mydb").
      option("driver","com.mysql.jdbc.Driver").
      option("user","root").
      option("password","123456").
      option("dbtable","film").load().show()

    sparkSession.stop()

  }

  //读取linux数据库
  @Test
  def test2(): Unit = {
     sparkSession.read.format("jdbc").
       option("url","jdbc:mysql://hadoop102:3306/mydb").
       option("driver","com.mysql.jdbc.Driver").
       option("user","root").
       option("password","123456").
       option("dbtable","person").load().show()
  }

  //方式2 设置配置文件
  @Test
  def test3(): Unit = {
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","123456")

    val df: DataFrame = sparkSession.read.jdbc("jdbc:mysql://hadoop102:3306/mydb", "person", properties)

    //全表查询
    df.show()

    df.createTempView("person")

    sparkSession.sql("select name from person").show()
  }

  //写
  @Test
  def test4(): Unit = {

    import  sparkSession.implicits._
    //准备数据
    val persons = List(Person(2, "lisi", 22), Person(3, "wangwu", 99), Person(4, "zhaoliu", 18), Person(5, "guoba", 24))

    val ds: Dataset[Person] = persons.toDS()

    //连接数据库
    ds.write.
      option("url","jdbc:mysql://hadoop102:3306/mydb").
      option("user","root").
      option("driver","com.mysql.jdbc.Driver").
      mode("overwrite").
      option("password","123456").
      option("dbtable","person_copy1").format("jdbc").save()

  }
}
case class Person(id:Int,name:String,age:Int)