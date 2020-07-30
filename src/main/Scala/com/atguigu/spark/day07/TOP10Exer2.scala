package com.atguigu.spark.day07

import org.apache.spark.rdd.RDD

object TOP10Exer2 extends BaseApp {

  override var outPutPath: _root_.scala.Predef.String = "output/Top10Exer2"

  def main(args: Array[String]): Unit = {

    runApp{

      //按行读取文件
      val rdd1: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")

      //过滤不需要的字段--提取点击数
      val clirdd: RDD[(String, Int)] = rdd1.flatMap(line => {
        val words: Array[String] = line.split("_")
        if (words(6) != "-1") {
          List((words(6), 1))
        } else {
          Nil
        }
      })
      val clirdds: RDD[(String, Int)] = clirdd.reduceByKey(_ + _)

      //过滤不需要的字段--提取订单数
      val ordrdd: RDD[(String, Int)] = rdd1.flatMap(line => {
        val words: Array[String] = line.split("_")
        if (words(8) != "null") {
          val ords: Array[String] = words(8).split(",")
          for (elem <- ords) yield {
            (elem,1)
          }
        } else {
          Nil
        }
      })

      val ordrdds: RDD[(String, Int)] = ordrdd.reduceByKey(_ + _)


      //过滤不需要的字段--提取支付数
      val payrdd: RDD[(String, Int)] = rdd1.flatMap(line => {
        val words: Array[String] = line.split("_")
        if (words(10) != "null") {
          val ords: Array[String] = words(10).split(",")
          for (elem <- ords) yield {
            (elem,1)
          }
        } else {
          Nil
        }
      })

      val paydds: RDD[(String, Int)] = payrdd.reduceByKey(_ + _)


      //三个rdd进行拼接
      val rdd2: RDD[(String, ((Int, Option[Int]), Option[Int]))] = clirdds.leftOuterJoin(ordrdds).leftOuterJoin(paydds)

      //调整数据结构
      val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.map {
        case (category, ((clickCount, orderCount), payCount)) =>
          (category, (clickCount, orderCount.getOrElse(0), payCount.getOrElse(0)))
      }

      //排序--将rdd转为List集合
      val listresult: List[(String, (Int, Int, Int))] = rdd3.collect().toList

      //对集合进行排序
      val finalresult: List[(String, (Int, Int, Int))] = listresult.sortBy(x => x._2)(
        Ordering.Tuple3(Ordering.Int.reverse, Ordering.Int.reverse, Ordering.Int.reverse)).take(10)

      //转换为RDD输出到本地文件
      sparkContext.makeRDD(finalresult,1).saveAsTextFile(outPutPath)
    }

  }

}
