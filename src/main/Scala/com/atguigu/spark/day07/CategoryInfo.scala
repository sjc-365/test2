package com.atguigu.spark.day07

case class CategoryInfo(categor:String,var clickCount :Int,var orderCount :Int,var paykCount :Int){

  override def toString: _root_.java.lang.String =
    categor + "," + clickCount + "," + orderCount + "," + paykCount
}
