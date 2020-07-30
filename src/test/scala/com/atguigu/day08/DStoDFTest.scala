package com.atguigu.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.junit.Test

class DStoDFTest {


  @Test
  def test1(): Unit = {

    //读取配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //创建SparkConfig对象
    val sc = new SparkContext(conf)

    //创建sparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换
    //spark不是包名，是上下文环境对象名
    import sparkSession.implicits._

    // 集合转DF、DS
    val list = List(1, 2, 3, 4)
    val df: DataFrame = list.toDF()
    val ds: Dataset[Int] = list.toDS()
    df.show()
    ds.show()

    //RDD转DF、DS
    val rdd1: RDD[Int] = sc.makeRDD(list)
    val df1: DataFrame = rdd1.toDF()
    val ds1: Dataset[Int] = rdd1.toDS()
    df1.show()
    ds1.show()

    //自定义类的集合转DF、DS
    val df2: DataFrame = List(User("jack", 20), User("jack1", 20), User("jack2", 20)).toDF()
    val ds2: Dataset[Row] = df2.as("User")
    df2.show()
    ds2.show()

    //读取文件
    val df3: DataFrame = sparkSession.read.json("input/employees.json")
    df3.show()

    sparkSession.stop()

    sc.stop()


  }

  @Test
  def test2(): Unit = {
    //读取配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //创建SparkConfig对象
    val sc = new SparkContext(conf)

    //创建sparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换
    //spark不是包名，是上下文环境对象名
    import sparkSession.implicits._

    //读取文件
    val df3: DataFrame = sparkSession.read.json("input/employees.json")
    df3.show()

    //DF转rdd
    val rdd: RDD[Row] = df3.rdd
    //row表示一行，可以使用row(index)来获取其中的元素
    val rdd2: RDD[(String, Long)] = rdd.map {
      case a: Row => (a.getString(0), a.getLong(1))
    }

    rdd.collect().foreach(println)

    sparkSession.stop()

    sc.stop()
  }


  @Test
  def test3(): Unit = {
    //读取配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //创建SparkConfig对象
    val sc = new SparkContext(conf)

    //创建sparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换
    //spark不是包名，是上下文环境对象名
    import sparkSession.implicits._

    //
    val list = List(Person("jack", 2222.22), Person("jack1", 2222.22), Person("jack2", 2222.22), Person("jack3", 2222.22))

    val rdd1: RDD[Person] = sc.makeRDD(list)

    //toDF(列名，列名)
    rdd1.toDF("name","salary").show()
  }

  @Test
  def test4(): Unit = {
    //读取配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //创建SparkConfig对象
    val sc = new SparkContext(conf)

    //创建sparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换
    //spark不是包名，是上下文环境对象名
    import sparkSession.implicits._


    val list = List(Person("jack", 2222.22), Person("jack1", 2222.22), Person("jack2", 2222.22), Person("jack3", 2222.22))

    val rdd1: RDD[Person] = sc.makeRDD(list)

    //DS类型自带表结构和列名
    val ds: Dataset[Person] = sparkSession.sparkContext.makeRDD(list).toDS()
    ds.show()

    //DS转DF
    val df: DataFrame = ds.toDF("name", "salary")
    df.show()


  }

}
case class User(name:String,age:Int)
case class Person(name:String,salary:Double)

