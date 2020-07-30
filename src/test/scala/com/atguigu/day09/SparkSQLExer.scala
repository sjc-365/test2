package com.atguigu.day09

import org.apache.spark.sql.{SparkSession, functions}
import org.junit.Test

class SparkSQLExer {

  @Test
  def test1(): Unit = {

    val spark: SparkSession = SparkSession.builder().
      master("local[*]").
      appName("df").
      enableHiveSupport().
      config("spark.sql.warehouse.dir","hdfs://hadoop102:9820/user/hive/warehouse")
      .getOrCreate()

    import  spark.implicits._

    spark.sql("use mydb")

    val sql1=
      """
        |
        |select ci.* ,pin.product_name,uva.click_product_id
        |from product_info pin
        |join user_visit_action uva
        |on pin.product_id = uva.click_product_id
        |join city_info ci
        |on ci.city_id = uva.city_id
        |""".stripMargin

    /*//旧udtf继承类
    val myudff = new MyUDFT
    spark.udf.register("myudf",myudff)
*/
    //新udtf继承类
    val mynewudtf = new MyNewUDTF
    spark.udf.register("myudf",functions.udaf(mynewudtf))

    val sql2=
      """
        |
        |select area,product_name,count(*) clickcount,myudf(city_name) result
        |from t1
        |group by area,product_name,click_product_id
        |""".stripMargin


    val sql3=
      """
        |select area,product_name,result,clickcount,
        |rank() over(partition by area order by clickcount desc) rn
        |from t2
        |""".stripMargin

    val sql4=
      """
        |select area,product_name,clickcount,result
        |from t3
        |where rn <= 3
        |""".stripMargin



    spark.sql(sql1).createTempView("t1")
    spark.sql(sql2).createTempView("t2")
    spark.sql(sql3).createTempView("t3")
    spark.sql(sql4).show(false)
    spark.close()
  }

}
