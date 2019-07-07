package com.github.tiger.spark.nlp.corpus

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TermFrequency {

  case class Corpus(line: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Statistics[Corpus TermFrequency]")
      // yarn
      .set("spark.yarn.am.waitTime", "1000s")

    val spark = SparkSession.builder.config(conf).getOrCreate

    import spark.implicits._

    val df = spark.read.textFile("hdfs://spark-220/data/corpus")
    val corpus = df.map(line => {
      Corpus(line)
    })

    corpus.createOrReplaceTempView("corpus")

    val lines = spark.sql("select distinct(line) as line from corpus")

    val rdd = lines.flatMap(row => {
      row.get(0).toString.split(" ")
    }).rdd

    val sortRdd = rdd.map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(pair => {
        pair._2
      }, false)

    // Reshuffle（重新洗牌）, 等同"spark.default.parallelism", "1"
    sortRdd.repartition(1)
      .saveAsNewAPIHadoopFile("hdfs://spark-220/output/corpus",
        classOf[Text], classOf[LongWritable], classOf[TextOutputFormat[Text, LongWritable]])

    spark.stop()
  }

}
