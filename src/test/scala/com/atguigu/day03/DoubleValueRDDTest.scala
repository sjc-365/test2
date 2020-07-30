package com.atguigu.day03

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

class DoubleValueRDDTest {

  //intersection方法：交集
  @Test
  def test1(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)

    val value1: RDD[Int] = sparkContext.makeRDD(list1, 1)
    val value2: RDD[Int] = sparkContext.makeRDD(list2, 2)

    value1.intersection(value2).saveAsTextFile("output")

    sparkContext.stop()

  }

  //union方法：并集
  @Test
  def test2(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)

    val value1: RDD[Int] = sparkContext.makeRDD(list1, 1)
    val value2: RDD[Int] = sparkContext.makeRDD(list2, 1)

    value1.union(value2).saveAsTextFile("output")
    sparkContext.stop()

  }

  //subtract方法：差集
  @Test
  def test3(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)

    val value1: RDD[Int] = sparkContext.makeRDD(list1, 1)
    val value2: RDD[Int] = sparkContext.makeRDD(list2, 1)

    value1.subtract(value2).saveAsTextFile("output")

    sparkContext.stop()

  }

  //cartesian方法：笛卡尔积
  @Test
  def test4(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)

    val value1: RDD[Int] = sparkContext.makeRDD(list1, 2)
    val value2: RDD[Int] = sparkContext.makeRDD(list2, 1)

    value1.cartesian(value2).saveAsTextFile("output")

    sparkContext.stop()

  }

  //zip方法：拉链
  @Test
  def test5(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)

    val value1: RDD[Int] = sparkContext.makeRDD(list1, 2)
    val value2: RDD[Int] = sparkContext.makeRDD(list2, 2)
    value1.zip(value2).saveAsTextFile("output")

    sparkContext.stop()

  }

  //zipwithindex方法：与自身索引进行拉链
  @Test
  def test6(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list1 = List(1, 2, 3, 4)

    val value1: RDD[Int] = sparkContext.makeRDD(list1, 2)
    value1.zipWithIndex().saveAsTextFile("output")

    sparkContext.stop()

  }

  //zippartitions方法：与不同类型rdd进行拉链
  @Test
  def test7(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list1 = List(1, 2, 3, 4,5)
    val list2 = List('a','b','c','d')

    val value1: RDD[Int] = sparkContext.makeRDD(list1, 2)
    val value2: RDD[Char] = sparkContext.makeRDD(list2, 2)

    //使用zipPartitions方法和zipAll方法
    value1.zipPartitions(value2)((iter1,iter2)=>iter1.zipAll(iter2,0,'x')).saveAsTextFile("output")


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
