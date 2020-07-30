package com.atguigu.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlday01 {

  def main(args: Array[String]): Unit = {
    //创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //RDD=>DataFrame=>DataSet转换需要引入隐式转换规则，否则无法转换
    //spark不是包名，是上下文环境对象名
    import spark.implicits._

    val list = List(1, 2, 3, 4)
    val df: DataFrame = list.toDF()
    val ds: Dataset[Int] = list.toDS()
    df.show()
  }
}
