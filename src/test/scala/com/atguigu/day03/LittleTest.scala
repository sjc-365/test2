package com.atguigu.day03

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

class LittleTest {


  //计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
  @Test
  def test1(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 4, 5, 6, 8, 9, 0)
    val value: RDD[Int] = sparkContext.makeRDD(list, 3)

    val ints: List[Int] = List[Int]()

    val value1: RDD[Int] = value.mapPartitions(x => List(x.max).iterator)
    println(value1.sum())

    sparkContext.stop()

  }

  //计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
  @Test
  def test2(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(1, 2, 4, 5, 6, 8, 9, 0)

    val value: RDD[Int] = sparkContext.makeRDD(list, 3)

    //将一个分区数据转换为集合
    val value1: RDD[Array[Int]] = value.glom()

    //List(Array(),Array(),Array()),x是数组
    val value2: RDD[Int] = value1.map(x => x.max)
    println(value2.sum)

    sparkContext.stop()

  }

  //将List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组
  @Test
  def test3(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val strings = List("Hello", "hive", "hbase", "Hadoop")

    val value: RDD[String] = sparkContext.makeRDD(strings, 2)
    val value1: RDD[(Char, Iterable[String])] = value.groupBy(x => x.charAt(0))
    value1.saveAsTextFile("output")


    sparkContext.stop()

  }

  //从服务器日志数据apache.log中获取每个时间段(不考虑日期)访问量
  @Test
  def test4(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val value: RDD[String] = sparkContext.textFile("input/apache.log",1)

    val value1: RDD[String] = value.map(x => x.split(" ")(3))

    val value2: RDD[(String, Iterable[String])] = value1.groupBy(x => x.substring(11, 13))

    val value3: RDD[(String, Int)] = value2.map(x => (x._1, x._2.size))
    value3.saveAsTextFile("output")

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
