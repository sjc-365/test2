package com.atguigu.spark.day07

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

abstract class BaseApp {

  //抽象属性：输出目录
  var outPutPath:String

  //Spark上下文
  val sparkContext = new SparkContext(new SparkConf().setAppName("My app").setMaster("local[*]"))

  //删除已存在的输出目录
  def init():Unit={
    val system: FileSystem = FileSystem.get(new Configuration())

    val path = new Path(outPutPath)

    //判断如果输出目录存在就删除
    if (system.exists(path)){
      system.delete(path,true)
    }

  }

  //读取数据
  def getAllBeans()={
    val datas: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")

    val beans: RDD[BaseBean] = datas.map(line => {
      val words: Array[String] = line.split("_")
      BaseBean(
        words(0),
        words(1),
        words(2),
        words(3),
        words(4),
        words(5),
        words(6),
        words(7),
        words(8),
        words(9),
        words(10),
        words(11),
        words(12)
      )
    })

    beans

  }

  //运行核心程序 op
  def runApp(op: =>Unit) ={

    init()

    try{
      op
    }catch {
      case e:Exception => println("异常"+ e.getMessage)
    }finally {
      sparkContext.stop()
    }

  }



}
