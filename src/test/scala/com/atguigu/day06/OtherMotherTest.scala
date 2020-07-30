package com.atguigu.day06

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

import scala.collection.mutable

class OtherMotherTest {


  //累加器--引入闭包变量
  @Test
  def test1(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5)

    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    //Driver端引入普通变量
    var sum = 0

    value.foreach(println)

    println(sum)

    sparkContext.stop()

  }

  //累加器--使用默认累加器
  @Test
  def test2(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5)

    //上下文设置累加器
    val acc: LongAccumulator = sparkContext.longAccumulator("mysum")

    //给累加器设置值
    acc.add(10)

    val value: RDD[Int] = sparkContext.makeRDD(list, 2)

    //在算子中使用累加器
    value.foreach(x => acc.add(x))

    println(acc.value)

    Thread.sleep(10000000)

    sparkContext.stop()

  }

  //累加器--使用自定义累加器:针对其他类型的的累加计数
  @Test
  def test3(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List("hello", "hello", "hi", "hi", "hello")

    val rdd: RDD[String] = sparkContext.makeRDD(list, 2)

    //创建累加器
    val acc = new MyCount

    //注册自定义累加器
    sparkContext.register(acc)

    //使用
    rdd.foreach(x => acc.add(x))

    //获取结果
    println(acc.value)

    sparkContext.stop()

  }

  @Test
  def test4(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list1 = List(1, 2, 3, 4,5,6,7,8,9,10)

    // 假设很大
    val list2 = List(5,6,7,8,9,10)

    //广播变量
    val value: Broadcast[List[Int]] = sparkContext.broadcast(list2)

    val value1: RDD[Int] = sparkContext.makeRDD(list1, 2)

    val value2: RDD[Int] = value1.filter(x => value.value.contains(x))

    value2.collect().foreach(println)

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


private class MyCount extends AccumulatorV2[String, mutable.Map[String, Int]] {

  //提供一个Map将累加后的结果返回到其中
  var mymap: mutable.Map[String, Int] = mutable.Map()

  //判断初始值是否为零
  override def isZero: Boolean = mymap.isEmpty

  //拷贝数据到Task中
  override def copy(): _root_.org.apache.spark.util.AccumulatorV2[_root_.scala.Predef.String, _root_.scala.collection.mutable.Map[_root_.scala.Predef.String, Int]] = new MyCount

  //重置初始值
  override def reset(): Unit = mymap.clear()

  //求和计算
  override def add(v: _root_.scala.Predef.String): Unit = {
    mymap.put(v, mymap.getOrElse(v, 0) + 1)

  }

  //合并累加器
  override def merge(other: _root_.org.apache.spark.util.AccumulatorV2[_root_.scala.Predef.String, _root_.scala.collection.mutable.Map[_root_.scala.Predef.String, Int]]): Unit = {

    //获取其他分区的累加器
    val value1: mutable.Map[String, Int] = other.value

    //遍历value，进行累加
    for ((word, count) <- value1) {

      mymap.put(word, mymap.getOrElse(word, 0) + count)

    }
  }

  //获取累加后的值
  override def value: _root_.scala.collection.mutable.Map[_root_.scala.Predef.String, Int] = mymap
}