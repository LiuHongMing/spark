package com.github.tiger.spark.elastic

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

import scala.collection.Map

object EsReader {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Elasticsearch example").setMaster("local[*]")
      .set("es.nodes", "175.63.101.107,175.63.101.108,175.63.101.109")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")

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

  }

}
