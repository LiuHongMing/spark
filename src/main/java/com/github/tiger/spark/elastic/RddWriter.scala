package com.github.tiger.spark.elastic

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

object RddWriter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Spark Elasticsearch example").setMaster("local")
      .set("es.nodes", "175.63.101.107,175.63.101.108,175.63.101.109")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")

    val sc = new SparkContext(conf)

    /**
      * 写入数据
      */
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

    /**
      * 写入 json 数据
      */
    val json1 = """{"id": 1, "spark": "dev", "job": "write"}"""
    val json2 = """{"id": 2, "elastic": "dev", "job": "store"}"""

    sc.makeRDD(Seq(json1, json2)).saveJsonToEs("spark/json")

    /**
      * 写入数据, 使用 EsSpark, 使用样例类
      */
    case class Trip(departure: String, arrival: String)
    val upcomingTrip = Trip("OTP", "SFO")
    val lastweekTrip = Trip("MUC", "OTP")

    val esRDD = sc.makeRDD(Seq(upcomingTrip, lastweekTrip));

    /**
      * 对于需要指定文档的id（或其他元数据字段，如ttl或timestamp）的情况，
      * 可以通过设置适当的映射即es.mapping.id来实现。
      */
    EsSpark.saveToEs(esRDD, "spark/docs", Map("es.mapping.id" -> "departure"))

  }

}
