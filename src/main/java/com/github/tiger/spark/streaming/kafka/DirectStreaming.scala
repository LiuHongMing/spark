package com.github.tiger.spark.streaming.kafka

import java.util.Random

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

class DirectStreaming {
}

object DirectStreaming extends App {

  val logger = LoggerFactory.getLogger(classOf[DirectStreaming])

  val conf = new SparkConf()
    .setAppName("Kafka streaming")
    .setMaster("local[*]")

  val ssc = new StreamingContext(conf, Seconds(1))
  ssc.sparkContext
    .hadoopConfiguration.set("fs.defaultFS", "hdfs://spark-220:9000")

  /**
    * 开启 checkpoint
    */
  ssc.checkpoint("/streaming/checkpoint")

  val topics = List("streaming-spark-2")
  val kafkaParams = collection.Map[String, Object](
    "bootstrap.servers" ->
      "kafka126:9092,kafka128:9092,kafka129:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "streaming-group-2",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> "false"
  )
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc, PreferConsistent,
    Subscribe[String, String](topics, kafkaParams))

  val random = new Random(1)
  val weight = () => {
    "%.2f".format(random.nextDouble()).toDouble
  }

  val pairStream = stream.map(record => {
    (record.value(), weight())
  })

  val printFunc = (r: RDD[(String, Double)]) => {
    val collect = r.collect()
    for (i <- collect) {
      println(i._1, i._2)
    }
  }

  pairStream.foreachRDD(printFunc)

  val windowStream = pairStream.countByWindow(Seconds(10), Seconds(5))

  windowStream.print(100)

  val timeInMs = System.currentTimeMillis()
  val prefix = s"hdfs://spark-220:9000/streaming/$timeInMs"
  val suffix = "kafka"
  pairStream.saveAsTextFiles(prefix, suffix)

  ssc.start()
  ssc.awaitTermination()

}


