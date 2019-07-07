package com.github.tiger.kafka.workbench.scala

import java.util

import org.apache.kafka.clients.consumer._

import scala.collection.JavaConverters._

object KafkaConsumer extends App {

  val props = new util.HashMap[String, Object]()
  props.put("bootstrap.servers",
    "kafka126:9092,kafka128:9092,kafka129:9092")
  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "my-group-xx")
  props.put("auto.offset.reset", "earliest")
  props.put("enable.auto.commit", "false")

  val consumer = new KafkaConsumer[String, String](props)

  val topics = List[String]("streaming").asJava
  consumer.subscribe(topics)

  val _timeout = 1000
  while (true) {
    val records = consumer.poll(_timeout)
    for (record <- records.asScala) {
      println("----------", record.key(), record.value(), "----------")
    }
    consumer.commitSync()

  }

  consumer.close()
}
