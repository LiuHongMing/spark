package com.github.tiger.spark.elastic

import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.Map

object EsClient {

  val conf = new SparkConf()
    .setAppName("Spark Elasticsearch example").setMaster("local")
    .set("es.nodes", "175.63.101.107,175.63.101.108,175.63.101.109")
    .set("es.port", "9200")
    .set("es.index.auto.create", "true")

  def main(args: Array[String]): Unit = {
    EsClient.read
  }

  def read = {
    val sc = new SparkContext(conf)

    /**
      * Query DSL
      */
    val compoundQuery =
      """{"query" : {
        |   "bool" : {
        |     "must" : {
        |         "term" : {
        |           "university_id" : "1223"
        |         }
        |     },
        |     "filter" : {
        |       "range" : {
        |         "action_datetime" : {
        |           "gte" : "2018-06-01",
        |           "lt" : "2018-07-01"
        |         }
        |       }
        |     }
        |   }
        |}}""".stripMargin

    println(compoundQuery)

    case class Behavior(itemId: String, majorId: String) {
      def apply(itemId: String, majorId: String): Behavior =
        new Behavior(itemId, majorId)
    }

    val esRDD = sc.esRDD("user_behavior/logs", compoundQuery)

    val behaviorRDD = esRDD.map((x: (String, Map[String, AnyRef])) => {

      val userId = x._2.getOrElse("user_id", "").asInstanceOf[String]
      val itemId = x._2.getOrElse("item_id", "").asInstanceOf[String]
      val majorId = x._2.getOrElse("major_id", "").asInstanceOf[String]

      (userId, Behavior(itemId, majorId))

    })

    behaviorRDD.persist(StorageLevel.MEMORY_ONLY)

    val f = (x: (String, Behavior)) => {
      println(x._1, x._2)
    }
    behaviorRDD.foreach(f)

    println("-----", "transformation _ map", "-----")

    val behaviorGroupRDD = behaviorRDD.groupByKey()
    behaviorGroupRDD.foreach((a: (String, Iterable[Behavior])) => {
      println("-----", a._1, "-----")
      val bIter = a._2

      for (b <- bIter) {
        println(b.itemId, b.majorId)
      }
    })

    println("-----", "transformation _ groupByKey", "-----")

    behaviorGroupRDD.cache()

    sc.stop()
  }

  def write = {

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
    val json1 =
      """{"id": 1, "spark": "dev", "job": "write"}"""
    val json2 = """{"id": 2, "elastic": "dev", "job": "store"}"""

    sc.makeRDD(Seq(json1, json2)).saveJsonToEs("spark/json")

    /**
      * 写入数据, 使用 EsSpark, 使用样例类
      */
    case class Trip(id: String, departure: String, arrival: String)
    val upcomingTrip = Trip(RandomStringUtils.randomAlphabetic(6), "OTP", "SFO")
    val lastweekTrip = Trip(RandomStringUtils.randomAlphabetic(6), "MUC", "OTP")

    val esRDD = sc.makeRDD(Seq(upcomingTrip, lastweekTrip));

    /**
      * 对于需要指定文档的id（或其他元数据字段，如ttl或timestamp）的情况，
      * 可以通过设置适当的映射即es.mapping.id来实现。
      */
    EsSpark.saveToEs(esRDD, "spark/docs", Map("es.mapping.id" -> "id"))

    sc.stop()
  }

}
