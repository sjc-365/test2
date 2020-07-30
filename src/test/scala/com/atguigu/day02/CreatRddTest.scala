package com.atguigu.day02

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class CreatRddTest {

  //将内存数据转换为RDD格式，并打印
  @Test
  def test1(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local")

    val sparkContext = new SparkContext(conf)

    val ints = List(1, 2, 3, 4)

    //将集合转化为RDD格式
    val value: RDD[Int] = sparkContext.parallelize(ints)

    //使用行动算子
    println(value.collect().mkString(","))

    //关闭资源
    sparkContext.stop()

  }

  //将内存数据输出到磁盘中,
  @Test
  def test2(): Unit = {

    //默认核数为当前机器CPU核数
    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5, 7, 8, 9)

    //设置分区数
    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    value.saveAsTextFile("output")

    sparkContext.stop()

  }

  //读取磁盘文件，RDD将转换为HadoopRDD
  //文件夹下不能嵌套子文件夹
  @Test
  def test3(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local")

    val sparkContext = new SparkContext(conf)

    val value: RDD[String] = sparkContext.textFile("input/hello",1)

    //执行行动算子
    value.saveAsTextFile("output")

    sparkContext.stop()

  }

  @Before
  def init(): Unit = {
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val path = new Path("output")

    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }


}
