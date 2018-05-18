package com.xunji


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 线性回归1-小数据集
  * 公式：f(x) = ax1 + bx2
  */

object LinearReg {
  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志显示终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    // 设置运行环境
    val conf = new SparkConf().setAppName("LinearRegression").setMaster("local[4]").set("spark.sql.warehouse.dir","D:/SparkWarehouseDir")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile("D:\\LineReg\\lpsa2.txt",1)
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    // Building the model
    val numIterations = 100//迭代次数
    val stepSize = 0.00000001//步长
    val model = LinearRegressionWithSGD.train(parsedData, numIterations,stepSize)
//    for (i <- parsedData) println(i.label + ":" + i.features)

    //获取真实值与预测值
    val valuesAndPreds = parsedData.map { point =>
      //对系数进行预测
      val  prediction = model.predict(point.features)
      //按格式进行储存
      (point.label, prediction)
    }

    //打印权重
    var weights = model.weights
    println("模型的权重"+model.weights)
    println("模型的残差"+model.intercept)

    //save as file
    val isString = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val path = "D:\\LineReg\\" + isString + "\\results"
    valuesAndPreds.saveAsTextFile(path)

    val numCount = valuesAndPreds.count()
    println("样本数量是："+numCount)

    val MSE = valuesAndPreds.map {case(v, p) => math.pow((v - p), 2)}.mean()
//      .reduce(_ + _ ) / valuesAndPreds.count
    println("训练的数据集的均方误差是： " + MSE)

    // Save and load model
    model.save(sc,"D:\\LineReg\\model")
    val sameModel = LinearRegressionModel.load(sc, "D:\\LineReg\\model")
    println(sameModel)

    /* //通过模型预测模型
     val result = model.predict(Vectors.dense(2, 1))
     println("model weights:")
     //计算两个系数，并以向量形式保存
     println(model.weights)
     println(result)
 */
  }
}
