package com.xunji

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Means {
  def main(args: Array[String]): Unit = {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 设置运行环境
    val conf = new SparkConf().setAppName("Kmeans").set("spark.driver.memory","471859200").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    // 装载数据集
    val data = sc.textFile("D:\\数据\\附件三：新项目任务数据.txt", 1)
    val arr = data.map(s => s.split("\\|"))
//    arr.map(s => s(1).toDouble).groupBy(x => x).map(x => (x._2.size,x._1)).repartition(1).sortByKey(false).map(x => x._1).saveAsTextFile("D:\\数据\\分布.txt")
    val parsedData: RDD[linalg.Vector] = arr.map(s => Vectors.dense(s(1).toDouble,s(2).toDouble))
//    val parsedData: RDD[linalg.Vector] = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))

    // 将数据集聚类，3个类，300次迭代，进行模型训练形成数据模型
    val numClusters = 3
    val numIterations = 300
    val model = KMeans.train(parsedData, numClusters, numIterations)

    // 打印数据模型的中心点
    println("Cluster centers:")
    for (c <- model.clusterCenters) {
      println("  " + c.toString)
    }

    // 使用误差平方之和来评估数据模型
    val cost = model.computeCost(parsedData)
    println("误差平方之和 = " + cost)

    val file = new File("D:\\result_kmeans2")
    if(file.isDirectory){
      for(x <- file.listFiles()){
        x.delete()
      }
      file.delete()
    }

    // 交叉评估2，返回数据集和结果
    val result2 = data.map {
      line =>
        val arr = line.split("\\|")
        val linevectore = Vectors.dense(Array(arr(1).toDouble,arr(2).toDouble))
        val prediction = model.predict(linevectore)
        prediction + "|" + line
//        line + "    所属簇为：" + prediction
    }.saveAsTextFile("D:\\result_kmeans2")

  /*  // 使用模型测试单点数据
    println("Vectors 0.2 0.2 0.2 is belongs to clusters:" + model.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))
    println("Vectors 0.25 0.25 0.25 is belongs to clusters:" + model.predict(Vectors.dense("0.25 0.25 0.25".split(' ').map(_.toDouble))))
    println("Vectors 8 8 8 is belongs to clusters:" + model.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))*/

 /*   // 交叉评估1，只返回结果
    val testdata = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    val result1 = model.predict(testdata)
    result1.saveAsTextFile("D:\\result_kmeans1")*/



  }
}
