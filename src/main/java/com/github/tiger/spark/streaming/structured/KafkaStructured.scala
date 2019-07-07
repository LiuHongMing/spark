package com.github.tiger.spark.streaming.structured

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object KafkaStructured extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("StructuredKafka")

  val spark = SparkSession.builder
    .config(conf).getOrCreate

  import spark.implicits._

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka126:9092,kafka128:9092,kafka129:9092")
    .option("subscribe", "streaming-spark-2")
    .option("startingOffsets", "earliest")
    .load()

  df.printSchema()

  val messages = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)].groupBy("value").count()

  val query = messages.writeStream

    /**
      * 输出模式
      */
    .outputMode("complete")
    .format("console")
    .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
    .option("", "")
    .start()

  query.awaitTermination()

}
