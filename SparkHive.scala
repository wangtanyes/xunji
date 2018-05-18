package com.xunji

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}

object SparkHive {
  def main(args: Array[String]): Unit = {

    //  屏蔽不必要的日志显示在终端上，因为有好多了类都产生的日志，下面这两行只能屏蔽一部分
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    println(warehouseLocation)

    val conf = new SparkConf().set("spark.sql.warehouse.dir", warehouseLocation).set("spark.testing.memory", "2147480000").setMaster("local[4]")

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

//    下面的错误信息，就是一个日志，并不是错误，就可以通过下面这句话进行屏蔽
//    spark.sparkContext.setLogLevel("OFF")   //  这句话可以真正控制sql显示的日志

    import spark.implicits._
    import spark.sql
//    val sqlContext = spark.sqlContext
    val df = spark.table("lijietest")
    df.show()

//    设置hive的存储格式，可以是orc，parquet，textfile，默认textfile
    spark.sqlContext.setConf("hive.default.fileformat", "Orc")

    //    设置hive的动态分区
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict") //    可以将hive的安全措施设定为”strict”模式，这样一个针对分区表的查询如果没有对分区进行限制的话，会禁止提交作业。这里设置为nonstrict

    //  hive开启简单模式不启用mr
    spark.sqlContext.setConf("hive.fetch.task.conversion","more")

    // 创建一个hive分区表
//    df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
    df.write.mode(SaveMode.Overwrite).partitionBy("列名").bucketBy(10,"列名").saveAsTable("表名")

//    创建一个存储数据格式为parquet的hive表
//    sql(s"CREATE EXTERNAL TABLE hive_ints(key int) STORED AS PARQUET")

//    创建一个存储数据格式为parquet的hive表，并加载数据
      val dataDir = "/tmp/parquet_data"
    sql(s"CREATE EXTERNAL TABLE hive_ints(key int) STORED AS PARQUET LOCATION '$dataDir'")

//    显式指定文件格式： 加载 json 格式
    spark.read.format("json").load("D:\\shuju:\\path\\text.json")
    spark.read.option("","").parquet("")


  }
}
