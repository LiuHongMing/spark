package com.github.tiger.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("spark://spark-220:7077")

    val sc = new SparkContext(conf)

    /**
      * 读入数据
      */
    val textFile = sc.textFile(args(0));
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))

    sc.stop();
  }

}
