package com.github.tiger.spark.base

import com.github.tiger.hadoop.partition.MyPartitioner
import org.apache.hadoop.mapreduce.Partitioner
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("spark://spark-220:7077")

    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0), 1);
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    sc.hadoopConfiguration.setClass("mapreduce.job.partitioner.class",
      classOf[MyPartitioner], classOf[Partitioner[_, _]])
    counts.saveAsNewAPIHadoopDataset(sc.hadoopConfiguration)

  }

}
