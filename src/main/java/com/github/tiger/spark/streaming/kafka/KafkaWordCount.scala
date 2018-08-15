package com.github.tiger.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount {

  def main(args: Array[String]) {

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("log")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "kafka1:9092,kafka2:9092,kafka3:9092"
    )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, Seconds(30))
    wordCounts.print()
    wordCounts.saveAsTextFiles("hdfs://namenode:8020/kafka-wordcount/", "spark")

    ssc.start()
    ssc.awaitTermination()
  }

}
