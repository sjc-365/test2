package com.atguigu.day10

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

class DStreamTest {

  //transfrom转换成RDD格式
  @Test
  def test1(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aa").setMaster("local[*]")

    val context = new StreamingContext(conf, Seconds(4))

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 3333)

    val ds2: DStream[(String, Int)] = ds1.transform(rdd => {
      //进行RDD之间的运算，并生成一个RDD
      val rdd1: RDD[(String, Int)] = rdd.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)

      //返回RDD
      rdd1
    })

    ds2.print()

    context.start()

    context.awaitTermination()
  }

  //transfrom转换成DF格式
  @Test
  def test2(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aa").setMaster("local[*]")

    val context = new StreamingContext(conf, Seconds(4))

    //创建session对象
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 3333)

    val ds2: DStream[Row] = ds1.transform(rdd => {
      //转换成DF格式
      val df: DataFrame = rdd.toDF()
      //将采集区间的数据封装成表
      df.createOrReplaceTempView("a")

      val df1: DataFrame = session.sql("select * from a")

      //封装成rdd返回
      df1.rdd
    })

    ds2.print()

    context.start()

    context.awaitTermination()
  }

  //Join方法
  @Test
  def test3(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aa").setMaster("local[*]")

    val context = new StreamingContext(conf, Seconds(4))

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 3333)
    val ds2: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 2222)

    val ds3: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val ds4: DStream[(String, Int)] = ds2.flatMap(line => line.split(" ")).map((_, 2)).reduceByKey(_ + _)

    //join操作
    val result: DStream[(String, (Int, Int))] = ds3.join(ds4)

    result.print()

    context.start()

    context.awaitTermination()
  }

  //updateStateByKey方法:实现不同采集周期间的求和
  @Test
  def test4(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aa").setMaster("local[*]")

    val context = new StreamingContext(conf, Seconds(4))

    //设置ck目录
    context.checkpoint("sum")

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 3333)

    val result: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).
      map((_, 1)).updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))

    result.print()

    context.start()

    context.awaitTermination()
  }

  //reduceByKeyAndWindow方法
  @Test
  def test5(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aa").setMaster("local[*]")

    val context = new StreamingContext(conf, Seconds(3))

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 3333)

    val result: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).map((_, 1)).reduceByKeyAndWindow(_+_, windowDuration = Seconds(6), slideDuration = Seconds(3))

    result.print()

    context.start()

    context.awaitTermination()
  }

  //reduceByKeyAndWindow方法重载
  @Test
  def test6(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aa").setMaster("local[*]")

    val context = new StreamingContext(conf, Seconds(3))

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 3333)

    val result: DStream[(String, Int)] = ds1.flatMap(line => line.split(" ")).
      map((_, 1)).reduceByKeyAndWindow(_+_, _-_,windowDuration = Seconds(6), slideDuration = Seconds(3))

    result.print()

    context.start()

    context.awaitTermination()
  }

  //提起定义窗口 windows
  @Test
  def test7(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aa").setMaster("local[*]")

    val context = new StreamingContext(conf, Seconds(3))

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 3333)

    val result: DStream[(String, Int)] = ds1.window(Seconds(6)).flatMap(line => line.split(" ")).
      map((_, 1)).reduceByKey(_+_)

    result.saveAsHadoopFiles("a1","txt")

    context.start()

    context.awaitTermination()

  }

  //关闭DStream
  @Test
  def test8(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aa").setMaster("local[*]")

    val context = new StreamingContext(conf, Seconds(4))

    val ds1: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 3333)

    val ds2: DStream[(String, Int)] = ds1.transform(rdd => {
      //进行RDD之间的运算，并生成一个RDD
      val rdd1: RDD[(String, Int)] = rdd.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)

      //返回RDD
      rdd1
    })

    ds2.print()

    context.start()

    context.awaitTermination()

    //新起一个线程
    new Thread(){
      //设置守护线程
      setDaemon(true)

      //循环判断,如果是true则不断休眠循环
      while(!true){
        //
        Thread.sleep(1000)
      }
      //判断条件为false，执行关闭操作
      //关闭sparkcontext,等待数据处理完成后再关闭
      context.stop(true,true)

    }
  }




}
