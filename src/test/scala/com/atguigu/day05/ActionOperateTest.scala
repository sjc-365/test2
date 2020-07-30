package com.atguigu.day05

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

class ActionOperateTest {

  //
  @Test
  def test(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    sparkContext.stop()

  }


  @Before
  def init(): Unit = {
    val fileSystem: FileSystem = FileSystem.get(new Configuration())
    val path = new Path("output")

    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }

}
