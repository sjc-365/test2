package com.atguigu.spark.day07

import org.apache.spark.rdd.RDD

object TOP10Session extends BaseApp {
  override var outPutPath: _root_.scala.Predef.String = "output/Top10Session"

  def main(args: Array[String]): Unit = {
    runApp{
      //获取Top10类别
      val Top10: RDD[String] = sparkContext.textFile("output/Top10Exer3")

      val Top10c: RDD[String] = Top10.map(line => line.split(",")(0))

      val Top10list: List[String] = Top10c.collect().toList


      //获取Top10的session
      val rdd1: RDD[BaseBean] = getAllBeans()

      val rdd2: RDD[((String, String), Int)] = rdd1.flatMap(bean => {
        if (bean.click_category_id != -1 && Top10list.contains(bean.click_category_id)) {
          List(((bean.click_category_id, bean.session_id), 1))

        } else {
          Nil
        }

      })

      //对session进行统计
      val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey(_ + _)

      //对rdd数据类型进行转换
      val rdd4: RDD[(String, (String, Int))] = rdd3.map {
        case ((categoryName, sessionId), count) => (categoryName, (sessionId, count))
      }

      //分组，将品类相同的进行汇总
      val rdd5: RDD[(String, Iterable[(String, Int)])] = rdd4.groupByKey(1)

      //排序取前10
      val result: RDD[(String, List[(String, Int)])] = rdd5.mapValues(it => {
        it.toList.sortBy(x => -x._2).take(10)

      })

      //输出到本地文件
     result.saveAsTextFile(outPutPath)

    }
  }
}
