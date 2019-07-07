package com.github.tiger.spark.streaming.structured

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object NetworkStructuredWindow extends App {

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("StructuredNetwork")
    .getOrCreate()

  import spark.implicits._

  val windowSize = 15
  val slideSize = 5

  val windowDuration = s"$windowSize seconds"
  val slideDuration = s"$slideSize seconds"

  // Create DataFrame representing the stream of input lines from connection to host:port
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .option("includeTimestamp", true)
    .load()

  lines.printSchema()

  // Split the lines into words, retaining timestamps
  val words = lines.as[(String, Timestamp)].flatMap(line =>
    line._1.split(" ").map(word => (word, line._2))
  ).toDF("word", "timestamp")

  // Group the data by window and word and compute the count of each group
  val windowedCounts = words.groupBy(
    window($"timestamp", windowDuration, slideDuration), $"word"
  ).count().orderBy("window")

  // Start running the query that prints the windowed word counts to the console
  val query = windowedCounts.writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .option("truncate", "false")
    .start()

  println(query.id)
  for (p <- query.recentProgress){
    println(p.prettyJson)
  }

  query.awaitTermination()

}
