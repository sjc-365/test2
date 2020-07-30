package com.atguigu.day06

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Before, Test}

class ReadAndWriterTest {

  //内存数据的读写
  @Test
  def test1(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5)
    val value: RDD[Int] = sparkContext.makeRDD(list, 1)

    value.collect().foreach(println)

    sparkContext.stop()

  }

  //对硬盘文件的读写
  @Test
  def test2(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val value: RDD[String] = sparkContext.textFile("input/hello3.txt", 1)

    value.saveAsTextFile("output")

    sparkContext.stop()

  }

  //对象的写
  @Test
  def test3(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5)
    val value: RDD[Int] = sparkContext.makeRDD(list, 1)

    val value1: RDD[(Int, Int)] = value.map(x => (x, 1))

    value1.saveAsObjectFile("obj")
    sparkContext.stop()

  }

  //对象的读
  @Test
  def test4(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val value: RDD[(Int, Int)] = sparkContext.objectFile[(Int, Int)]("obj", 1)

    value.saveAsTextFile("output")
    sparkContext.stop()

  }

  //写SequnceFile--数据须是k-v类型
  @Test
  def test5(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 4, 5)
    val value: RDD[Int] = sparkContext.makeRDD(list, 1)

    val value1: RDD[(Int, Int)] = value.map(x => (x, 1))

    value1.saveAsSequenceFile("seq")

    sparkContext.stop()

  }

  //读SequnceFile
  @Test
  def test6(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val value: RDD[(Int, Int)] = sparkContext.sequenceFile[Int, Int]("seq", 1)


    value.collect().foreach(println)
    sparkContext.stop()

  }

  //读数据库
  @Test
  def test7(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    //创建JdbcRDD，RDD读取数据库中的数据

    /*class JdbcRDD[T: ClassTag](
                                sc: SparkContext,
                                getConnection: () => Connection,
                                sql: String,
                                lowerBound: Long,
                                upperBound: Long,
                                numPartitions: Int,
                                mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)*/

    val value = new JdbcRDD(sparkContext, () => {
      //注册驱动
      Class.forName("com.mysql.jdbc.Driver")
      //获取连接
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb", "root", "123456")
      connection

    }, "select * from film where film_id > ? and film_id < ?", 5, 9, 1,
      (rs: ResultSet) => {
        rs.getInt("film_id") + "----" + rs.getString("title") + "----" + rs.getString("description")
      }
    )

    value.collect().foreach(println)

    sparkContext.stop()

  }

  //写数据库
  @Test
  def test8(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    val list = List((11, "aaa", "sfsfdf"), (12, "bbb", "sdadfd"), (13, "ccc", "dasdasd"), (14, "ddd", "fgrtg"))

    val value: RDD[(Int, String, String)] = sparkContext.makeRDD(list, 2)

    value.foreachPartition(it => {
      Class.forName("com.mysql.jdbc.Driver")
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb", "root", "123456")

      val statement: PreparedStatement = connection.prepareStatement("insert into film(film_id,title,description) values (?,?,?)")

      it.foreach {
        case (x, y, z) => {
          statement.setInt(1, x)
          statement.setString(2, y)
          statement.setString(3, z)

          statement.execute()
        }
      }

      statement.close()
      connection.close()

    })
    sparkContext.stop()

  }

  //读Hbase
  @Test
  def test9(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    //获取Hbase配置对象
    val conf1: Configuration = HBaseConfiguration.create()

    //指定要读取哪个表
    conf1.set(TableInputFormat.INPUT_TABLE, "t2")

    //读取Hbase,返回RDD
    val value: RDD[(ImmutableBytesWritable, Result)] = sparkContext.newAPIHadoopRDD(
      conf1, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val value1: RDD[String] = value.flatMap {
        //获取单行的信息
      case (rowkey, result) => {
        val cells: Array[Cell] = result.rawCells()
        for (elem <- cells) yield {
          val rk: String = Bytes.toString(CellUtil.cloneRow(elem))
          val cf: String = Bytes.toString(CellUtil.cloneFamily(elem))
          val cq: String = Bytes.toString(CellUtil.cloneQualifier(elem))
          val v: String = Bytes.toString(CellUtil.cloneValue(elem))

          rk + ":" + cf + ":" + cq + ":" + v
        }
      }
    }

    value1.collect().foreach(println)

    sparkContext.stop()

  }

  @Test
  def test10(): Unit = {

    val conf = new SparkConf().setAppName("My app").setMaster("local[*]")

    val sparkContext = new SparkContext(conf)

    //配置文件
    val conf1: Configuration = HBaseConfiguration.create()
    //要写入哪个表
    conf1.set(TableOutputFormat.OUTPUT_TABLE, "T2")

    // 设置让当前的Job使用TableOutPutFormat，指定输出的key-value类型
    val job: Job = Job.getInstance(conf1)

    //设置输出格式
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //指定k-v类型
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    //准备数据
    val list = List(("r3", "cf1", "age", "20"), ("r3", "cf1", "name", "marray1"), ("r4", "cf1", "age", "20"), ("r4", "cf1", "name", "tony"))

    val rdd: RDD[(String, String, String, String)] = sparkContext.makeRDD(list, 2)

    //在RDD中封装写出数据
    val value1: RDD[(ImmutableBytesWritable, Put)] = rdd.map {
      case (rowkey, cf, cq, v) => {
        val key = new ImmutableBytesWritable()
        key.set(Bytes.toBytes(rowkey))

        val value = new Put(Bytes.toBytes(rowkey))
        value.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes(v))

        (key, value)

      }
    }

    value1.saveAsNewAPIHadoopDataset(job.getConfiguration)

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
