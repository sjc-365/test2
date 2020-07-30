package com.atguigu.day03

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

class SingleValueRDDTest {


  //map方法
  @Test
  def test1(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4)

    val value: RDD[Int] = sparkContext.makeRDD(list)
    val value1: RDD[Int] = value.map(x => x + 1)

    println(value1.collect().mkString(","))

    sparkContext.stop()

  }

  //将内存数据存储到磁盘中
  @Test
  def test2(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4)

    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    value.saveAsTextFile("output")

    sparkContext.stop()

  }

  //从服务器日志数据apache.log中获取用户请求URL资源路径
  @Test
  def test3(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val value: RDD[String] = sparkContext.textFile("input/apache.log", 1)

    val value1: RDD[String] = value.map(x => x.split(" ")(6))

    value1.saveAsTextFile("output")
    sparkContext.stop()

  }

  //mapPartitions方法  将分区中的奇数写入到数据库
  @Test
  def test4(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(1, 2, 3, 4)

    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    val value1: RDD[Int] = value.mapPartitions(x => x.filter(elem => elem % 2 == 1))

    value1.saveAsTextFile("output")
    sparkContext.stop()

  }

  //mappartitionswithindex方法 获取第二个分区的数据
  @Test
  def test5(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(1, 2, 3, 4)

    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    val value1: RDD[Int] = value.mapPartitionsWithIndex {
      case (index, iter) if (index == 1) => iter
      case _ => Nil.iterator
    }

    value1.saveAsTextFile("output")

    sparkContext.stop()

  }

  //flatmap方法 将单个Int转换为List
  @Test
  def test6(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(List(1, 2), 3, List(4, 5))
    val value: RDD[Any] = sparkContext.makeRDD(list, 2)

    val value1: RDD[Any] = value.flatMap {
      //属于List集合，元素类型、个数无所谓
      case x: List[_] => x
      case y: Int => List(y)
    }

    value1.saveAsTextFile("output")

    sparkContext.stop()

  }

  //glom()方法
  @Test
  def test7(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5)

    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    val value1: RDD[Array[Int]] = value.glom()

    //打印各个分区集合的数据
    value1.collect().foreach(x => println(x.mkString(",")))

    sparkContext.stop()

  }

  //filter方法 过滤
  @Test
  def test8(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4)
    val value: RDD[Int] = sparkContext.makeRDD(list)
    val value1: RDD[Int] = value.filter(x => x % 2 == 0)
    println(value1.collect().mkString(","))

    sparkContext.stop()

  }

  //groupby 分组
  @Test
  def test9(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 2, 1, 2, 2)

    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    //相同key下的value会形成一个迭代器
    val value1: RDD[(String, Iterable[Int])] = value.groupBy(x => x + "a")

    value1.saveAsTextFile("output")

    sparkContext.stop()

  }

  //sample方法
  @Test
  def test10(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(1, 2, 3, 4, 3, 2, 1, 2, 2, 2)

    val value: RDD[Int] = sparkContext.makeRDD(list, 1)

    value.sample(true, 0.5).saveAsTextFile("output")

    sparkContext.stop()

  }

  //distinct方法 去重
  @Test
  def test11(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 9, 1, 2, 7, 8)
    val value: RDD[Int] = sparkContext.makeRDD(list, 2)
    //重新设置分区数：1
    value.distinct(1).saveAsTextFile("output")


    sparkContext.stop()

  }

  //coalesce方法 合并分区
  @Test
  def test12(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 89, 3)
    val value: RDD[Int] = sparkContext.makeRDD(list, 4)

    //将分区数减少到两个
    //val value1: RDD[Int] = value.coalesce(2)。saveAsTextFile("output")

    //将分区数增加到6个
    value.coalesce(6, true).saveAsTextFile("output")

    sparkContext.stop()

  }

  //repartition方法 扩大分区
  @Test
  def test13(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(4, 5, 6, 7)
    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    //suffle之后重新分区，分区策略是??
    value.repartition(4).saveAsTextFile("output")

    sparkContext.stop()

  }

  //sortBy
  @Test
  def test14(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(4, 5, 6, 7)
    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    value.sortBy(x => x ).saveAsTextFile("output")

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
