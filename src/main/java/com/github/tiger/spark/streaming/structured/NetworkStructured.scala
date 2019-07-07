package com.github.tiger.spark.streaming.structured

import org.apache.spark.sql.SparkSession

object NetworkStructured extends App {

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("StructuredNetwork")
    .getOrCreate()

  import spark.implicits._

  /**
    * 创建表示从连接到 localhost: 9999 的输入行 stream 的 DataFrame
    *
    * nc -lk 9999
    */
  val lines = spark.read
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // 将 lines 切分为 words
  val words = lines.as[String].flatMap(_.split(" "))

  // 生成正在运行的 word count
  val wordCounts = words.groupBy("value").count()

  // 开始运行将 running counts 打印到控制台的查询
  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  println(query.lastProgress)

  query.awaitTermination()

}
