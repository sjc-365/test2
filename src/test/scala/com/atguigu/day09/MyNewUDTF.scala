package com.atguigu.day09

import java.text.DecimalFormat

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/*
* input:城市名
* buffer:（map（城市名，数量），总数）
* output:
* */
class MyNewUDTF extends Aggregator[String,Mybuffer,String]{

  //初始化
  override def zero: Mybuffer = Mybuffer(Map[String,Long](),0l)

  //分区内计算
  override def reduce(b:Mybuffer, a:String): Mybuffer = {
    val map: Map[String, Long] = b.map
    val value: Long = map.getOrElse(a,0l)+1l

    b.map=map.updated(a,value)

    b.sum+=1

    b

  }

  //分区间计算
  override def merge(b1: Mybuffer, b2: Mybuffer): Mybuffer = {
    val map1: Map[String, Long] = b1.map
    val map2: Map[String, Long] = b2.map

    val result: Map[String, Long] = map2.foldLeft(map1) {
      case (map, (city, count)) => {
        val value: Long = map.getOrElse(city, 0l) + count
        map.updated(city, value)
      }
    }

    b1.map = result

    b1.sum+=b2.sum

    b1

  }

  private val format = new DecimalFormat("0.00%")

  //返回结果
  override def finish(reduction:  Mybuffer): String = {
    val top2: List[(String, Long)] = reduction.map.toList.sortBy(-_._2).take(2)

    val othercount: Long = reduction.sum -top2(0)._2 - top2(1)._2

    //将其他map加入
    val top3: List[(String, Long)] = top2 :+ ("其他",othercount)

    //拼接字符串

    val res:String = ""+top3(0)._1+format.format(top3(0)._2/reduction.sum.toDouble)+
      " " +top3(1)._1+format.format(top3(1)._2/reduction.sum.toDouble)+
      " " +top3(2)._1+format.format(top3(2)._2/reduction.sum.toDouble)

    res

  }

  //buffer的编码器
  override def bufferEncoder: Encoder[Mybuffer] = Encoders.product

  //输出的编码器
  override def outputEncoder: Encoder[String] = Encoders.STRING
}
case class Mybuffer(var map:Map[String,Long],var sum:Long)
