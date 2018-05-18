package com.xunji

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object FenLei {
  def main(args: Array[String]): Unit = {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 设置运行环境
    val conf = new SparkConf().setAppName("Kmeans").set("spark.driver.memory","471859200").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val file = new File("D:\\数据\\聚类")
    if(file.isDirectory){
      for(x <- file.listFiles()){
        x.delete()
      }
      file.delete()
    }

    val secoData = sc.textFile("D:\\result_kmeans2\\part-00000")
    val secoArr = secoData.map(x => x.split("\\|"))
    secoArr.map(x => (x(0),Array(x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))).repartition(1).groupBy(x=>x._1).filter(x => x._1.toInt == 2).map(x =>{
        x._2.map(arr => arr._2(0))
    }).saveAsTextFile("D:\\数据\\聚类")
  }
}
