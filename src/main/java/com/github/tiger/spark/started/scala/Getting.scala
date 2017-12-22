package com.github.tiger.spark.started.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD操作: transformation（转换）, action（动作）
  *
  * @author liuhongming
  */
object Getting {

  val conf = new SparkConf().setAppName("Getting").setMaster("local[*]")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val data = List(1, 2, 3, 4, 5)
    println(data)

    val distData = parallelize(data)
    printf("RDD.count()\n%s\n", distData.count())

    // transformation
    map(distData)
    reduceByKey()
    filter()

    // action
    reduce(distData)

  }

  def parallelize(data: List[Int]): RDD[Int] = {
    // 分布式数据集
    val distData = sc.parallelize(data, 2)
    distData
  }

  // ========== transformation ==========

  def map(rdd: RDD[Int]): RDD[Int] = {
    val addOne = (a: Int) => a + 1
    println("RDD.map()")
    val result = rdd.map(addOne)
    result.foreach(x => printf("%s ", x))
    println()
    result
  }

  def reduceByKey(): RDD[(Int, Int)] = {
    val data = List((1, 2), (3, 4), (3, 6))
    // 分布式数据集
    val distData = sc.parallelize(data, 2)
    val result = distData.reduceByKey((x, y) => x + y)
    printf("RDD.reduceByKey()\n%s\n", data)
    result.foreach(x => printf("%s ", x))
    println()
    result
  }

  def filter(): Unit = {
    val days = List("Sunday", "Monday", "Tuesday", "Wednesday",
      "Thursday", "Friday", "Saturday")
    val when = "AM" :: "PM" :: days
    val distData = sc.parallelize(when, 3)
    printf("RDD.filter()\n%s\n", when)
    distData.filter(x => x.length <= 2).foreach(
      x => printf("%s ", x)
    )
    println()
  }

  def flatMapValues(): Unit = {
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    println(b.flatMapValues("x" + _ + "x").foreach(x => print(x)))
  }

  // ========== action ==========

  def reduce(rdd: RDD[Int]): Unit = {
    val result = rdd.reduce((x, y) => x + y)
    println("RDD.reduce()")
    rdd.foreach(x => printf("%s ", x))
    printf("\n%s\n", result)
  }

}
