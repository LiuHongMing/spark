package com.github.tiger.spark.base

import org.apache.spark.SparkContext

import scala.collection.mutable

object WordCountResult {

  val USER = "root"

  def main(args: Array[String]): Unit = {

    /**
      * 设置访问Spark使用的用户名
      */
    System.setProperty("user.name", USER);

    /**
      * 设置访问Hadoop使用的用户名
      */
    System.setProperty("HADOOP_USER_NAME", USER);

    val env = mutable.Map[String, String]()
    /**
      * 为Spark环境中服务于本App的各个Executor程序
      *
      * 设置访问Hadoop使用的用户名
      */
    env += ("HADOOP_USER_NAME" -> USER);

    /**
      * 为Spark环境中服务于本App的各个Executor程序
      *
      * 设置使用内存量的上限
      */
    System.setProperty("spark.executor.memory", "1g");

    val jars = Seq[String]()
    val sc = new SparkContext("local", "WordCountResult", "/opt/spark-2.3", jars, env)

    val hadoopRDD = sc.wholeTextFiles("hdfs://spark-220:9000/word_count_result")

    println("RDD partition size: %s".format(hadoopRDD.partitions.size))

    val flatFn = (tuple: (String, String)) => {

      val result = mutable.HashMap[String, Int]()

      val tokens = tuple._2.split("[\n\r]")
      for {

        token <- tokens

      } {
        val len = token.length

        val delimiter = token.lastIndexOf(',')
        val word: String = token.substring(1, delimiter)
        val count: Int = Integer.parseInt(token.substring(delimiter + 1, len - 1))

        result.update(word, count)
      }

      result

    }

    val flatRDD = hadoopRDD.flatMap(flatFn)

    val flatDisplay = (f: (String, Int)) => {
      println(f._1, f._2)
    }
    flatRDD.foreach(flatDisplay)

    val filterFn = (tuple: (String, Int)) => {
      val key = tuple._1

      val isMatch = !key.isEmpty && key.matches("[\\d\\w]*")

      isMatch
    }

    println("----------")

    val filterRDD = flatRDD.filter(filterFn)

    val filterDisplay = (f: (String, Int)) => {
      println(f._1, f._2)
    }
    filterRDD.foreach(filterDisplay)

    val reduceFn = (x: (String, Int), y: (String, Int)) => {
      ("total_count_valid", x._2 + y._2)
    }

    val reduceAction = filterRDD.reduce(reduceFn)

    println("The calculation results: (%s, %s)"
      .format(reduceAction._1, reduceAction._2))

    Thread.sleep(60 * 60 * 1000)
  }

}
