package com.atguigu.spark.day07

import java.io

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object TOP10Exer1 extends BaseApp {
  override var outPutPath: _root_.scala.Predef.String = "output/Top10Exer1"


  def main(args: Array[String]): Unit = {
    runApp{

      //读取文件数据，并将数据转换为Bean类型
      val rdd1: RDD[BaseBean] = getAllBeans()

      //获取（品类，（点击数，下单数，支付数））
      val rdd2: RDD[(String, (Int, Int, Int))] = rdd1.flatMap(
        bean => {
          if (bean.click_category_id != "-1") {
            //转换为集合
            List((bean.click_category_id, (1, 0, 0)))

          } else if (bean.order_category_ids != "null") {
            val categorys: Array[String] = bean.order_category_ids.split(",")

            val tuples: Array[(String, (Int, Int, Int))] = categorys.map(x => (x, (0, 1, 0)))
            tuples

          } else if (bean.pay_category_ids != "null") {
            val categorys1: Array[String] = bean.pay_category_ids.split(",")

            val tuples: Array[(String, (Int, Int, Int))] = categorys1.map(x => (x, (0, 0, 1)))
            tuples

          } else {

            Nil
          }
        }
      )

      //分组合并 reduceBykey
      val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.reduceByKey {
        case ((d1, o1, p1), (d2, o2, p2)) => {
          (d1 + d2, o1 + o2, p1 + p2)
        }
      }


      //排序 sortby
      val result: Array[(String, (Int, Int, Int))] = rdd3.sortBy(x => x._2, numPartitions = 2)(Ordering.Tuple3(Ordering.Int.reverse, Ordering.Int.reverse, Ordering.Int.reverse)
        , ClassTag(classOf[Tuple3[Int, Int, Int]])).take(10)

      //输出
      sparkContext.makeRDD(result,1).saveAsTextFile(outPutPath)

    }
  }
}
