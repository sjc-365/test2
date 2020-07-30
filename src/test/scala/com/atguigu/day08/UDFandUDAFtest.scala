package com.atguigu.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession, TypedColumn, functions}
import org.junit.Test

class UDFandUDAFtest {

  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  import sparkSession.implicits._


  //UDF的使用
  @Test
  def test1(): Unit = {

    val df: DataFrame = sparkSession.read.json("input/employees.json")

    //定义注册udf函数
    sparkSession.udf.register("myudf",(name:String) => "hello:" + name)

    //建表
    df.createTempView("emp")

    //使用SQL
    sparkSession.sql("select myudf(name) from emp").show()


    sparkSession.stop()
  }

  //使用UDAF 求和
  @Test
  def test2(): Unit = {
    val df: DataFrame = sparkSession.read.json("input/employees.json")

    //定义函数
    val mySum = new mySum

    //注册函数
    sparkSession.udf.register("mySum",mySum)

    //建表
    df.createTempView("emp")

    //使用函数
    sparkSession.sql("select mySum(salary) from emp").show()

    sparkSession.stop()

  }

  //使用UDAF 求平均值
  @Test
  def test3(): Unit = {
    val df: DataFrame = sparkSession.read.json("input/employees.json")

    val ds: Dataset[user3] = df.as[user3]

    //定义函数
    val myAvg = new myAvg

    //注册UDAF函数 新版Aggregator
    sparkSession.udf.register("myAvg",functions.udaf(myAvg))

    //建表
    ds.createTempView("emp")

   //将UDAF函数转为一个列名
    val column: TypedColumn[user3, Double] = myAvg.toColumn

    //
    ds.select(column).show()

    //使用函数
    sparkSession.sql("select myAvg(name,salary) from emp").show()

    sparkSession.stop()
  }
}

//继承老版本UserDefinedAggregateFunction
class mySum extends UserDefinedAggregateFunction{

  //输入数据类型
  override def inputSchema: StructType = StructType( StructField("input",DoubleType)::Nil)

  //buffer数据类型
  override def bufferSchema: StructType = StructType( StructField("sum",DoubleType)::Nil)

  //输出数据类型
  override def dataType: DataType = DoubleType

  //是否确定性
  override def deterministic: Boolean = true

  //初始化buffer
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0.0

  //分区内数据运算
  override def update(buffer:  MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = input.getDouble(0)+buffer.getDouble(0)

  }

  //分区间数据合并
  override def merge(buffer1:  MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)

  }

  //结果返回值
  override def evaluate(buffer:  Row): Any = buffer(0)
}

//三个泛型来确定输入输出buffer数据类型
class myAvg extends Aggregator[user3,Mybuffer,Double]{

  //初始化缓冲区
  override def zero: Mybuffer = Mybuffer(0.0,0)

  //分区内数据运算
  override def reduce(b: Mybuffer, a: user3): Mybuffer = {
    b.sum += a.salary
    b.count+=1

    b
  }

  //分区间数据运算
  override def merge(b1: Mybuffer, b2: Mybuffer): Mybuffer = {
    b1.count+=b2.count
    b1.sum+=b2.sum

    b1
      }

  //返回结果
  override def finish(reduction: Mybuffer): Double = {
    reduction.sum/reduction.count

  }

  // buffer的Encoder类型
  override def bufferEncoder: Encoder[Mybuffer] = ExpressionEncoder[Mybuffer]

  // 最终返回结果的Encoder类型
  override def outputEncoder: Encoder[Double] = ExpressionEncoder[Double]
}

case class user3(name:String ,salary:Double)

//缓存数据类型
case class Mybuffer(var sum:Double , var count:Int)

