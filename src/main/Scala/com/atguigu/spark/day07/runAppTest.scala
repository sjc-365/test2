package com.atguigu.spark.day07

object runAppTest extends BaseApp {

  override var outPutPath: _root_.scala.Predef.String = "output/wc"

  def main(args: Array[String]): Unit = {
    runApp{
      val list = List(1, 2, 3, 4, 5)
      sparkContext.makeRDD(list,2).saveAsTextFile(outPutPath)

    }

  }

}
