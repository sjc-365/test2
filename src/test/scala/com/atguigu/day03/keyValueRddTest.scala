package com.atguigu.day03

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkConf, SparkContext}
import org.junit.{Before, Test}

class keyValueRddTest {

  //partitionby方法 HashPartitioner分区器
  @Test
  def test1(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List((1, 1.1), (2, 1.2), (2, 1.1), (1, 1.2))

    val value: RDD[(Int, Double)] = sparkContext.makeRDD(list, 2)

    val value1: RDD[(Int, Double)] = value.partitionBy(new HashPartitioner(3))

    value1.saveAsTextFile("output")

    sparkContext.stop()

  }

  //partitionby方法 RangePartitioner分区器
  @Test
  def test2(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List((1, 1.1), (2, 1.2), (2, 1.1), (1, 1.2))

    val value: RDD[(Int, Double)] = sparkContext.makeRDD(list, 2)

    //传入分区数，及rdd集合
    val value1: RDD[(Int, Double)] = value.partitionBy(new RangePartitioner(3, value))

    value1.saveAsTextFile("output")

    sparkContext.stop()

  }

  //partitionby方法 自定义分区器
  @Test
  def test3(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(Person("jack", 21), Person("jack1", 16), Person("jack2", 31),
      Person("tom", 21), Person("tom1", 22), Person("bob", 21))

    val value: RDD[Person] = sparkContext.makeRDD(list, 1)
    //将单值类型转换为k-v类型,Bean类为key
    val value1: RDD[(Person, Int)] = value.map(x => (x, 1))

    //传入自定义分区器，并且传入分区数
    val value2: RDD[(Person, Int)] = value1.partitionBy(new MyPartitioner(2))

    value2.saveAsTextFile("output")

    sparkContext.stop()

  }

  //mapValues方法
  @Test
  def test4(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(("a", 1), ("b", 1), ("a", 1), ("c", 1), ("a", 1))

    val value: RDD[(String, Int)] = sparkContext.makeRDD(list, 2)

    val value1: RDD[(String, Int)] = value.mapValues(x => x * 3)

    value1.saveAsTextFile("output")

    sparkContext.stop()

  }

  //groupByKey方法 根据key进行分组
  @Test
  def test5(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(("a", 1), ("b", 1), ("a", 1), ("c", 1), ("a", 1))

    val value: RDD[(String, Int)] = sparkContext.makeRDD(list, 2)

    val value1: RDD[(String, Iterable[Int])] = value.groupByKey()

    value1.saveAsTextFile("output")

    sparkContext.stop()

  }

  //reducebykey 根据key进行分组后，对value进行运算
  @Test
  def test6(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(("a", 1), ("b", 1), ("a", 1), ("c", 1), ("a", 1))

    //这里的x,y都是相同key的情况下，value的代表
    sparkContext.makeRDD(list, 2).reduceByKey((x, y) => x + y).saveAsTextFile("output")

    sparkContext.stop()

  }

  //aggregateBykey方法
  @Test
  def test7(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(("a", 1), ("b", 2), ("c", 3), ("c", 3), ("a", 1))

    val value: RDD[(String, Int)] = sparkContext.makeRDD(list, 2)

    value.aggregateByKey("hello:")((u, x) => u + x, (u, x) => x + u + 1).saveAsTextFile("output")
    sparkContext.stop()

  }

  //foldBykey方法
  @Test
  def test8(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(("a", 1), ("c", 3), ("b", 2), ("c", 3), ("a", 1))

    val value: RDD[(String, Int)] = sparkContext.makeRDD(list, 2)

    value.foldByKey(5)((u, x) => u + x).saveAsTextFile("output")
    sparkContext.stop()

  }

  /*combineBykey方法/
  //对key进行分组，将零值赋值给分组后中的第一个value
  //对分区内对value进行聚合
  //再对分区间的value进行聚合
  */
  @Test
  def test9(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(("a", 1), ("c", 3), ("b", 2), ("c", 3), ("a", 1))

    val value: RDD[(String, Int)] = sparkContext.makeRDD(list, 2)


    //参数需要明确类型 将第一个value替换成5，再进行分区内运算，分区间运算
    val value1: RDD[(String, Int)] = value.combineByKey((value: Int) => 5, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y)

    value1.saveAsTextFile("output")

    sparkContext.stop()

  }


  //sortbykey方法 根据key进行排序
  @Test
  def test10(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list = List(("a", 1), ("c", 3), ("b", 2), ("c", 3), ("a", 1))

    val value: RDD[(String, Int)] = sparkContext.makeRDD(list, 2)

    //value.sortByKey().saveAsTextFile("output")
    value.sortByKey(false).saveAsTextFile("output")
    sparkContext.stop()

  }

  //sortbykey方法 提供隐式转换变量，将自定义类转换为可排序的
  @Test
  def test11(): Unit = {

    implicit val ord: Ordering[Person1] = new Ordering[Person1] {
      override def compare(x: Person1, y: Person1): Int = {
        var result: Int = x.name.compareTo(y.name)
        if (result == 0) {
          result = x.age.compareTo(y.age)
          result
        } else {
          result
        }
      }
    }

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(Person1("jack", 21), Person1("jack1", 16), Person1("jack2", 31),
      Person1("tom", 21), Person1("tom1", 22), Person1("bob", 21))

    val value: RDD[Person1] = sparkContext.makeRDD(list, 1)

    val value1: RDD[(Person1, Int)] = value.map(x => (x, 1))

    value1.sortByKey().saveAsTextFile("output")

    sparkContext.stop()
  }

  //join方法
  @Test
  def test12(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list1 = List(("a", 1), ("b", 1), ("f", 1), ("c", 1))
    val list2 = List(("a", 1), ("b", 1), ("c", 1), ("e", 1))
    val value1: RDD[(String, Int)] = sparkContext.makeRDD(list1, 1)
    val value2: RDD[(String, Int)] = sparkContext.makeRDD(list2, 2)

    //leftOuterJoin方法 不同RDD中，将key相同的value进行关联
    value1.leftOuterJoin(value2).saveAsTextFile("output")

    sparkContext.stop()

  }

  //cogroup: 先每个RDD内部，根据key进行聚合，将相同key的value聚合为Iterable，再在rdd之间，根据相同key聚合
  @Test
  def test13(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)
    val list1 = List((1, "a"), (2, "b"), (3, "c"), (5, "e"), (1, "aa"), (2, "bb"))

    //  (1,List("a1"))
    val list2 = List((1, "a1"), (2, "b1"), (3, "c1"), (4, "d1"), (3, "c11"), (4, "d11"))

    val rdd1: RDD[(Int, String)] = sparkContext.makeRDD(list1, 2)
    val rdd2: RDD[(Int, String)] = sparkContext.makeRDD(list2, 2)

    rdd1.cogroup(rdd2).saveAsTextFile("output")

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


class MyPartitioner(nums: Int) extends Partitioner {
  /*自定义分区器
* 继承抽象类 Partitioner
* 重写属性和方法
* 属性：分区数
* 方法:分区规则
* */
  override def numPartitions: Int = nums

  override def getPartition(key: Any): Int = {
    if (!key.isInstanceOf[Person]) {
      0
    } else {
      val person: Person = key.asInstanceOf[Person]

      person.age.hashCode() % numPartitions
    }

  }
}

case class Person(name: String, age: Int)

case class Person1(name: String, age: Int)



