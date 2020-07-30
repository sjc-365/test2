package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRdd {

  def main(args: Array[String]): Unit = {
    //默认分区数为1
    val conf: SparkConf = new SparkConf().setAppName("My RDD").setMaster("local")
    val context = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 6)
    //将集合转化为RDD格式
    val value: RDD[Int] = context.makeRDD(list)
    //将数据输出到本地
    println(value.collect().mkString(","))

    //关闭资源
    context.stop()

  }

}
