package com.xunji

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random


object KafkaPro {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "47.98.34.131:9092")
    props.put("acks", "all")
    props.put("retries", new Integer(0))
    props.put("batch.size", new Integer(16384))
    props.put("linger.ms", new Integer(1))
    props.put("buffer.memory", new Integer(33554432))
//    props.put("","")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    var i = 0
    while(true){
      val name = (0x4e00 + (Math.random * (0x9fa5 - 0x4e00 + 1)).toInt).toChar
      val age = scala.util.Random.nextInt(100)
      val width = new Random(System.currentTimeMillis()).nextInt(100) + "千克"
      val sex = if(new Random(System.currentTimeMillis() - new Random().nextInt(8888888)).nextInt(2) == 1) "男" else  "女"
      val json = s"{'name':'${name.toString}','age':${age.toString},'width':'${width.toString}','sex':'${sex.toString}'}"

      var message = new ProducerRecord[String,String]("wangtan",json,Random.nextDouble().toString)
      producer.send(message)
      Thread.sleep(1000)
      println(message)
      i+=1
    }
  }
}
