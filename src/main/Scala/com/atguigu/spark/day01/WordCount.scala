package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf = new SparkConf().setAppName("My app")

    //创建连接对象
    val context = new SparkContext(conf)

    //读取文件,并生成PDD类型的文件
    val value: RDD[String] = context.textFile(args(0))

    //转换数据类型
    val value1: RDD[String] = value.flatMap(x => x.split(" "))

    //分组
    val value2: RDD[(String, Iterable[String])] = value1.groupBy(x => x)

    //计数
    val value3: RDD[(String, Int)] = value2.map(x => (x._1, x._2.size))

    //将数据采集到内存中
    val tuples: Array[(String, Int)] = value3.collect()

    //打印输出结果
    println(tuples.mkString(","))

    //关闭Spark连接
    context.stop()
  }
}
