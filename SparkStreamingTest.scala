package com.xunji

import java.util

import com.alibaba.fastjson.JSON
import com.fasterxml.jackson.databind.ObjectMapper
import com.scalaTools.{C3P0Connection, ScalaConnectionPool}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}   //  这个可能需要手动导入，因为识别不出来

import scala.collection.mutable


object SparkStreamingTest {

  //屏蔽不必要的日志显示在终端上
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("NetWorkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
//    val ds = ssc.socketTextStream("192.168.123.243", 10000)
//    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val kafka = KafkaUtils.createStream(ssc,"47.98.34.131:2181","test-consumer-group",Map("wangtan" -> 2),StorageLevel.MEMORY_AND_DISK_SER)

    kafka.foreachRDD(x =>{
      x.foreachPartition(partionRecords =>{
        partionRecords.foreach(everyRecord => {
          var  json:String = everyRecord._1
//         这里是json字符串转化为Map，map必须是Java.util,不能是scala中的。如果转化为实体类，就是‘new 实体类().getCalss’
          val map = JSON.parseObject(json,new util.HashMap[String,String]().getClass)
          printf("name = %s, age = %s, width = %s, sex =%s\n",map.get("name"),map.get("age"),map.get("width"),map.get("sex"))
        })
      })
    })


/*//  kafkaStreaming转化为DataFrame
    kafka.foreachRDD(rdd => {
      //    下面第一行是会报错的原因是spark.sql.warehouse.dir路径不对,因为D:/Spark Project/dongrong/spark-warehous的Spark Project中间用一个空格，导致识别不出来,可以路径中空格去掉或者下面的方式
      val spark = SparkSession.builder.master("local[2]").config(rdd.sparkContext.getConf).getOrCreate()
//      val spark = SparkSession.builder.master("local[2]").config("spark.sql.warehouse.dir","D:/SparkProjectTest").appName("rdd transform to DF").getOrCreate()
      import  spark.implicits._
      val kafkaStreamDF = rdd.toDF("key", "value")  //  参数的意思是给列命名，key即是_1,value即是_2
      kafkaStreamDF.createOrReplaceTempView("DF")
      spark.sql("select * from DF").show()
    })*/

/*//    测试foreachRDD与foreachPartition
    kafka.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords =>{
        val connect = ScalaConnectionPool.getConnection
        val stmt = connect.createStatement()
        partitionOfRecords.foreach(record =>{
          stmt.addBatch("insert into searchKeyWord (insert_time,keyword,search_count) values (now(),'"+record._1+"','"+record._2+"')")
        })
        stmt.executeBatch()
        connect.commit()
      })
    })*/

/*//    测试foreachRDD与foreachPartition，通过连接c3p0数据库连接池，存储数据
    kafka.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords =>{
//        创建的connect一定要在‘foreachPartition’里面，不然会出现错误的
        val connect = C3P0Connection.getConnection
        val sql = "insert into `student` (`name`, `age`) values (?, ?)"
        val ps = connect.prepareStatement(sql)
        while(partitionOfRecords.hasNext){
          val record = partitionOfRecords.next()
          ps.setString(1,record._1)
          ps.setString(2,record._2)
          ps.addBatch()
          println(record._1,record._2)
        }
        ps.executeBatch()
        C3P0Connection.releaseSources(connect,ps,null)
      })
    })*/

//    kafka.window(Seconds(10)).repartition(1).saveAsTextFiles("start","end")
//    kafka.window(Seconds(10)).foreachRDD(x => {
//      x.map(x => (x._1,x._2)).foreach(println(_))
//    })
//    kafka.foreachRDD(x => {
//      x.map(x => (x._1,x._2)).foreach(println(_))
//    })
//    kafka.map(x => x).foreachRDD(println(_))
//    kafka.flatMap(x => (x._1)).foreachRDD(println(_))
    //打印结果
//    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
