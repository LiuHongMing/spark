package spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("FileWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    val lines = ssc.textFileStream("hdfs://namenode:8020/text")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
//    val wordCounts = words.map(word => (word, 1)).reduceByKey(
//      (v1: Int, v2: Int) => {
//        v1 + v2
//      }
//    )

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
