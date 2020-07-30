package com.atguigu.spark.day07

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

//继承AccumulatorV2，需确定输入输出数据类型

//输入类型（类别，（次数，次数，次数）） ---(String,String)

//输出类型Map((String,(Int,Int,Int))......)

class MyTop10Acc extends AccumulatorV2[(String,String),mutable.Map[String,CategoryInfo]]{

  private val map: mutable.Map[String, CategoryInfo] = mutable.Map[String, CategoryInfo]()

  //判断是否为空
  override def isZero: Boolean = map.isEmpty

  //复制数据过来
  override def copy(): _root_.org.apache.spark.util.AccumulatorV2[(_root_.scala.Predef.String, _root_.scala.Predef.String), _root_.scala.collection.mutable.Map[_root_.scala.Predef.String, _root_.com.atguigu.spark.day07.CategoryInfo]] = new MyTop10Acc

  //重置
  override def reset(): Unit = map.clear()

  //分区内累加
  override def add(v:  (_root_.scala.Predef.String, _root_.scala.Predef.String)): Unit = {

    //获取品类ming
    val categoryName: String = v._1

    val field: String = v._2

    //将数据累加到map
    val info: CategoryInfo = map.getOrElse(categoryName, CategoryInfo(categoryName, 0, 0, 0))

    //累加
    field match {
      case "click" => info.clickCount += 1
      case "order" => info.orderCount += 1
      case "pay" => info.paykCount += 1
      case _ =>
    }

    map.put(categoryName,info)
  }

    //分区间累加
  override def merge(other:  _root_.org.apache.spark.util.AccumulatorV2[(_root_.scala.Predef.String, _root_.scala.Predef.String), _root_.scala.collection.mutable.Map[_root_.scala.Predef.String, _root_.com.atguigu.spark.day07.CategoryInfo]]): Unit = {

    val otherpar: mutable.Map[String, CategoryInfo] = other.value

    for ((categoryName,categoryBean) <- otherpar) {

      val info1: CategoryInfo = map.getOrElse(categoryName, CategoryInfo(categoryName, 0, 0, 0))

      info1.clickCount += categoryBean.clickCount
      info1.orderCount += categoryBean.orderCount
      info1.paykCount += categoryBean.paykCount

      map.put(categoryName,info1)

    }

  }

  //返回值
  override def value: _root_.scala.collection.mutable.Map[_root_.scala.Predef.String, _root_.com.atguigu.spark.day07.CategoryInfo] = map
}
