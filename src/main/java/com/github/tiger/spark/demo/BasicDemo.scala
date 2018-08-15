package com.github.tiger.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

object BasicDemo extends App {

  val conf = new SparkConf().setAppName("RddDemo").setMaster("local[3]")

  val sc = new SparkContext(conf)

  val rdd = sc.parallelize(1 to 9, 3)

  def mapFunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next
    while (iter.hasNext) {
      val cur = iter.next
      res :+= (pre, cur)
      pre = cur
    }
    res.iterator
  }

  val mapRdd = rdd.mapPartitions(mapFunc)

  val res = mapRdd.collect
  res.foreach((f: (Int, Int)) => {
    print(f.toString)
  })


}
