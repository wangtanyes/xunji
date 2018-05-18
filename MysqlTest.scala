package com.xunji

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object MysqlTest {

  def getConnect() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    // DriverManager.getConnection("jdbc:mysql://localhost:3306/world?user=root&password=123456");
    DriverManager.getConnection("jdbc:mysql://192.168.123.243:3306/amc?", "datadepartment", "datacode");
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("masql test").setMaster("local");
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("").master("local").getOrCreate()

    //  ?是占位符，后面三个是参数，前两个都是对应的占位符中的值，第三个是分区数，r代表sql语句查询出来的结果集，操作就是获取结果集然后返回

    val sql = "SELECT bb.title, cc.title as field, cc.content from amc_card aa LEFT JOIN amc_cardinfo bb on aa.id = bb.card_id " +
      "LEFT JOIN amc_cardinfo_detail cc on bb.id = cc.cardinfo_id " +
      "where 1 = ? and 2 = ? and aa.`name` LIKE '定价-029-抵质押物-评估录入%' and cc.title != ''"

    val sqll = "select * from amc_card where 1 = ? and 2 = ?"

    val result = new JdbcRDD(sc, getConnect, sql, 1, 2, 1, r => {
      var title = r.getString("title")
      title
    })

    //    result.foreach(println(_))


  }
}
