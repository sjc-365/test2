package com.atguigu.day09

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

/*输入：  string : cityname
*         buffer:  Map[String,Long] 保存每个城市的点击次数
*                  Long ： 计算此商品在此地区的点击总数
*
*         输出：  string :  北京21.2%，天津13.2%，其他65.6%
*/
class MyUDFT extends UserDefinedAggregateFunction{

  private val format = new DecimalFormat("0.00%")

  //输入数据类型
  override def inputSchema: StructType = StructType(StructField("city",StringType)::Nil)

  //缓冲区数据类型
  override def bufferSchema: StructType = StructType(StructField("map",MapType(StringType,LongType))::
    StructField("sum",LongType)::Nil )

  //输出数据类型
  override def dataType: DataType = StringType

  //是否确定性
  override def deterministic: Boolean = true

  //初始化缓冲区
  override def initialize(buffer:  MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0l
  }

  //分区内数据运算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    //当前城市点击数为1
    val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)

    //取出input的key
    val key: String = input.getString(0)
    //进行累加操作
    val value: Long = map.getOrElse(key, 0l) + 1l

    buffer(0) = map.updated(key,value)

    //总次数累加
    buffer(1) = buffer.getLong(1)+1

  }

  //分区间数据运算
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //取出map
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)

    //
    val result: collection.Map[String, Long] = map2.foldLeft(map1) {
      case (map, (city, count)) => {
        val sumcount: Long = map.getOrElse(city, 0l) + count
        map.updated(city, sumcount)

      }

    }
    buffer1(0) = result

    buffer1(1) = buffer2.getLong(1)+ buffer1.getLong(1)

  }

  //返回值
  override def evaluate(buffer: Row): Any = {
    val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
    val counts: Long = buffer.getLong(1)

    val top2: List[(String, Long)] = map.toList.sortBy(-_._2).take(2)

    //计算其他的数量
    val othercount: Long = counts - top2(0)._2 - top2(1)._2

    //加入其他
    val result: List[(String, Long)] = top2 :+ ("其他",othercount)

    //拼接字符串，输出要求的格式
    val lastresult:String = ""+result(0)._1+""+format.format(result(0)._2/counts.toDouble)+
      result(1)._1+""+format.format(result(1)._2/counts.toDouble)+
    result(2)._1+""+format.format(result(2)._2/counts.toDouble)

    lastresult

  }
}
