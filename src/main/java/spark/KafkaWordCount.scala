package spark

import kafka.serializer.StringDecoder
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object KafkaWordCount {

  def main(args: Array[String]) {

    //    if (args.length < 4) {
    //      System.err.println("Usage KafkaWordCount: <zkQuorum> <group> <topics> <numThreads>")
    //      System.exit(1)
    //    }
    //    val zkQuorum = "192.168.20.37:2181,192.168.20.38:2181,192.168.20.39:2181"
    //    val groupId = "LOG"
    //    val topics = "log"
    //    val numThreads = "1"
    //    //    val Array(zkQuorum, group, topics, numThreads) = args
    //    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    //    val ssc = new StreamingContext(sparkConf, Seconds(30))
    //    ssc.checkpoint("hdfs://namenode:8020/tmp")
    //
    //    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //    val lines = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap).map(_._2)
    //    val words = lines.flatMap(_.split(" "))
    //    val wordCounts = words.map(x => (x, 1L))
    //      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(30), 2)

    //    val kafkaTopics = Set("log")
    //    val kafkaParams = Map[String, String](
    //      "bootstrap.servers" -> "kafka1:9092,kafka2:9092,kafka3:9092",
    //      "group.id" -> "LOG",
    //      "serializer.class" -> "kafka.serializer.StringEncoder",
    //      "auto.commit.enable" -> "true",
    //      "auto.offset.reset" -> "smallest"
    //    )

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
