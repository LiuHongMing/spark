package com.github.tiger.spark.streaming.structured

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.Trigger

object KafkaStructuredWindow extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("StructuredKafka")

  val spark = SparkSession.builder
    .config(conf).getOrCreate

  import spark.implicits._

  val windowSize = 15
  val slideSize = 5

  val windowDuration = s"$windowSize seconds"
  val slideDuration = s"$slideSize seconds"

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka126:9092,kafka128:9092,kafka129:9092")
    .option("subscribe", "streaming-spark-2")
    .option("startingOffsets", "earliest")
    .option("includeTimestamp", true)
    .load()

  df.printSchema()

  val messages = df.selectExpr("CAST(value AS STRING)", "timestamp")
    .as[(String, Timestamp)].groupBy(
    window($"timestamp", windowDuration, slideDuration), $"value"
  ).count()

  val query = messages.writeStream

    /**
      * 输出模式
      */
    .outputMode("complete")
    .format("console")
    .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
    .option("truncate", "false")
    .start()

  query.awaitTermination()

}
