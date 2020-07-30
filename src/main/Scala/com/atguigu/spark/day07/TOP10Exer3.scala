package com.atguigu.spark.day07

import com.atguigu.spark.day07.TOP10Exer1.getAllBeans
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object TOP10Exer3 extends BaseApp {
  override var outPutPath: _root_.scala.Predef.String = "output/Top10Exer3"

  def main(args: Array[String]): Unit = {
    runApp{

      //创建累加器
      val acc = new MyTop10Acc

      //注册
      sparkContext.register(acc)

      //调用累计器

      val rdd1: RDD[BaseBean] = getAllBeans()

      rdd1.foreach(bean => {

        if (bean.click_category_id != "-1") {
          acc.add(bean.click_category_id,"click")


        } else if (bean.order_category_ids != "null") {
          val categorys: Array[String] = bean.order_category_ids.split(",")

          for (elem <- categorys) {acc.add(elem,"order")}

        } else if (bean.pay_category_ids != "null") {
          val categorys1: Array[String] = bean.pay_category_ids.split(",")

          for (elem <- categorys1) {acc.add(elem,"pay")}

        } else {

          Nil
        }

      })

      //获取累加值
      val result: mutable.Map[String, CategoryInfo] = acc.value

      //排序
      val list: List[CategoryInfo] = result.values.toList

      val finalresult: List[CategoryInfo] = list.sortBy(x => (x.clickCount, x.orderCount, x.paykCount))(Ordering.Tuple3(Ordering.Int.reverse, Ordering.Int.reverse, Ordering.Int.reverse)).take(10)

      //输出到本地文件

      sparkContext.makeRDD(finalresult,1).saveAsTextFile(outPutPath)







    }
  }
}
