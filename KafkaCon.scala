package com.xunji

import java.util
import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.serializer.StringDecoder
import kafka.utils.VerifiableProperties

object KafkaCon {


  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("zookeeper.connect", "192.168.123.241:2181") //  zookeeper 配置，通过zk 可以负载均衡的获取broker
    /*props.put(ConsumerConfig.GROUP_ID_CONFIG,"test-consumer-group")   //  group 代表一个消费组
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")    // 自动提交设置为true， 也可以为false
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")    // 控制自动提交的频率
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS, "30000")   // 设置心跳时间
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"smallest") //  消息日志自动偏移量,防止宕机后数据无法读取
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin")
    props.put(ConsumerConfig.GROUP_ID_CONFIG,"test-consumer-group")
    //    props.put("serializer.class", "kafka.serializer.StringEncoder")       // 序列化类
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")*/

    props.put("zookeeper.session.timeout.ms", "10000")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("bootstrap.servers", "192.168.123.241:9092")
    props.put("group.id", "test-consumer-group")
    props.put("key.deserializer", "org.apache.kafka.common.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.StringDeserializer")
    props.put("auto.offset.reset", "smallest")

    val consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props))

    //  指定需要订阅的topic
    val topicCountMap = new util.HashMap[String, Integer]()
    topicCountMap.put("wangtan", new Integer(5))

    //  指定key的编码格式
    val keyDecoder = new StringDecoder((new VerifiableProperties()))

    //  指定value的编码格式
    val valueDecoder = new StringDecoder((new VerifiableProperties()));

    //获取topic 和 接受到的stream 集合
    val map = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder)

    //根据指定的topic 获取 stream 集合
    val kafkaStreams = map.get("wangtan");


  }
}
