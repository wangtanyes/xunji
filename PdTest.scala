package com.xunji

import java.util.Properties

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object PdTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("masql test").setMaster("local");
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().appName("").master("local").getOrCreate()

    import spark.implicits._
    val pro = new Properties()
    pro.put("user", "datadepartment")
    pro.put("password", "datacode")
    pro.put("driver", "com.mysql.jdbc.Driver")

    /*   使用options的方式读取数据 */
    /*    val jdbcDF = spark.read.format("jdbc").options(
          Map("url" ->  "jdbc:mysql://192.168.123.243:3306/amc?useUnicode=true&characterEncoding=UTF-8",
            "driver"->"com.mysql.jdbc.Driver",
            "user"->"datadepartment",
            "password"->"datacode",
            "dbtable" -> "amc_card",
            "fetchSize" -> "10000",
    //        "partitionColumn" -> "yeard",
    //        "lowerBound" -> "1988",
    //        "upperBound" -> "2016",
            "numPartitions"->"1"
          )).load()*/


    /*  测试  """ """的用法  */
    val str =
      """
        |123
        |54641
        |7897
        |554465
        |165145
        |
      """.stripMargin
    //    println(str)

    /*  测试spark.read.jdbc  分区的数量的等于 Array中写的条件的个数*/
    val url = "jdbc:mysql://192.168.123.243:3306/amc"
    val result = spark.read.jdbc(url, "amc_card", Array("status != 1"), pro)
    //    val pars = result.rdd.partitions.size     //  查询当前有多少分区
    /* 创建一个临时视图 */
    result.createOrReplaceTempView("card")
    val r = spark.sql("select * from card")
    //    r.show(r.count().toInt)
    //    result.select($"id", $"status" + 1).show(result.count().toInt)
    //    result.filter($"status" > 0).show()


    /*  测试DataSet */
    val caseClaseDs = Seq(Persion("zhangsan", 123)).toDS()
    //    caseClaseDs.show()

    val primitiveDs = Seq(1, 2, 3).toDS()
    //    primitiveDs.map(_+1).show()
    //    primitiveDs.map(_+1).collect().foreach(println(_))

    val path = "C:\\spark\\people.json"
    val peopleDs = spark.read.json(path).as[Persion]
    //    peopleDs.show()

    /*  将RDD转化为Datasets */
    /*  方法一 */

    val peopleDF = spark.sparkContext.textFile("C:\\spark\\people.txt").map(_.split(","))
      .map(attributes => Persion(attributes(0), attributes(1).trim.toInt)).toDF()

    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("select name, age from people where age between 3 and 99")
    //    teenagersDF.show()
    //    teenagersDF.map(teenager => "Name: " + teenager(0)).show()    //  通过索引位置拿到字段得值
    //    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("age")).show()  //  通过字段名字拿到字段的

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    //    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name","age"))).collect().foreach(println(_))  // 结果别转化为了map，不能再用show显示了，用collection来显示所有结果

    /*  方法二 */
    val peopleRDD = spark.sparkContext.textFile("C:\\spark\\people.txt")
    val schemaString = "name age"

    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
    val peopleDF2 = spark.createDataFrame(rowRDD, schema)
    peopleDF.createOrReplaceTempView("people")
    val results = spark.sql("SELECT name FROM people")
    //    results.map(attributes => "Name: " + attributes(0)).show()


    /*  创建dataFrame插入数据库  */
    val data = sc.parallelize(List((1, "name"), (2, "age"), (3, "score"))).map(x => Row.apply(x._1, x._2))
    val sch = StructType(StructField("value", IntegerType) :: (StructField("key", StringType) :: Nil))

    val df = spark.createDataFrame(data, sch)
    df.show()
    //    df.write.partitionBy("key").mode(SaveMode.Append).jdbc(url,"my",pro)    //  如果有分区调用此方法
    df.write.mode(SaveMode.Append).jdbc(url, "my", pro)


  }

  case class Persion(name: String, age: Long) //  case class 放在方法体里面会报错，要放在方法外面
}
