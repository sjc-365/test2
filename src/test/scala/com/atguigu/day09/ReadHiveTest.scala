package com.atguigu.day09

import org.apache.spark.sql.SparkSession
import org.junit.Test

class ReadHiveTest {

  System.setProperty("HADOOP_USER_NAME","atguigu")
  val spark: SparkSession = SparkSession.builder().
    master("local[*]").
    appName("df").
    enableHiveSupport().
    config("spark.sql.warehouse.dir","hdfs://hadoop102:9820/user/hive/warehouse")
    .getOrCreate()

  //本地模式
  @Test
  def test1(): Unit = {
    //spark.sql("create table mydb.person1(name string,age int)")
    //spark.sql("show databases").show()
    spark.sql("use mydb").show()
    spark.sql("show tables").show()
    spark.stop()
  }


}
