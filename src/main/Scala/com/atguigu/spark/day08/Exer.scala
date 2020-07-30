package com.atguigu.spark.day08

import org.apache.spark.sql.SparkSession

object Exer {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().
      master("local[*]").
      appName("df").
      enableHiveSupport().
      config("spark.sql.warehouse.dir","hdfs://hadoop102:9820/user/hive/warehouse")
      .getOrCreate()

    val sql1=
      """
        |select ci.* ,pin.product_name,uva.click_product_id
        |from product_info pin
        |join user_visit_action uva
        |on pin.product_id = uva.click_product_id
        |join city_info ci
        |on ci.city_id = uva.city_id
        |""".stripMargin

    val sql2=
      """
        |select area,product_name,count(*) clickcount
        |from t1
        |group by area,product_name,click_product_id
        |""".stripMargin


    val sql3=
      """
        |select area,product_name,clickcount,
        |rank() over(partition by area order by clickcount desc) rn
        |from t2
        |""".stripMargin

    val sql4=
      """
        |select area,product_name,clickcount
        |from t3
        |where rn <= 3
        |""".stripMargin

    spark.sql("use mydb")

    spark.sql(sql1).createTempView("t1")
    spark.sql(sql2).createTempView("t2")
    spark.sql(sql3).createTempView("t3")
    spark.sql(sql4).show()

    spark.close()
  }

}
