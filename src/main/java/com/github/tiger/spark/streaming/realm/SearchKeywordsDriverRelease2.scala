package com.github.tiger.spark.streaming.realm

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.github.tiger.scala.util.Md5Util
import com.github.tiger.spark.util.FastJsonUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark
import org.joda.time.DateTime

/**
  * 统计每天搜索关键字出现次数
  */
object SearchKeywordsDriverRelease2 {

  val _checkpoint = "hdfs://master/user/zpcampus/checkpoint"

  case class Keyword(timestamp: Long, keyword: String)

  case class KeywordCount(id: String, keyword: String, count: Long, createDate: String)

  def createContext(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName("Statistics[Search Keywords]")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("es.nodes", "172.30.5.17,172.30.5.18,172.30.5.19")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")

    // 降低batch时间
    val secInterval = 10

    val ssc = new StreamingContext(conf, Seconds(secInterval))

    val topics = List("campus_search_condition")
    val kafkaParams = collection.Map[String, Object](
      "bootstrap.servers" -> "172.30.100.90:9092,172.30.100.91:9092,172.30.100.92:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group-campus_search_condition",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "true"
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val keywords = stream.map(record => {

      val jsonValue = record.value()
      val jsonObject = JSON.parseObject(jsonValue)

      val writeData = jsonObject.get("data").toString
      val (ok, result) = FastJsonUtil.tryConvert[AsMessage](writeData)

      var keyword = ""
      result match {
        case res if res.isInstanceOf[AsMessage] =>
          keyword = result.asInstanceOf[AsMessage].getKeyword
        case _ => println("None")
      }

      val writeTime = jsonObject.get("writeTime").toString.toLong
      var writeTimeMillis = 0L

      if (ok && !"".equals(keyword)) {
        writeTimeMillis = new DateTime(writeTime)
          .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
          .withMillisOfSecond(0).getMillis
      }

      (Keyword(writeTimeMillis, keyword), 1)

    }).filter(kw => {
      val start = DateTime.now().withHourOfDay(0).withMinuteOfHour(0)
        .withSecondOfMinute(0).withMillisOfSecond(0).getMillis
      if (kw._1.timestamp == start) {
        true
      } else {
        false
      }
    }).reduceByKey((x, y) => x + y).cache()

    val updateState = (newValues: Seq[Int], old: Option[Int]) => {
      val prev = old.getOrElse(0)
      val curr = newValues.sum + prev

      Some(curr)
    }

    val stateful = keywords.updateStateByKey(updateState).checkpoint(Seconds(6 * secInterval))

    stateful.foreachRDD(rdd => {
      rdd.collect().foreach(pair => {
        val timestamp = pair._1.timestamp
        val keyword = pair._1.keyword
        val count = pair._2
        val createDate = new Date(timestamp)

        val id = Md5Util.md5(timestamp + "_" + keyword)

        val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+0800'")
        val keywordCount = KeywordCount(id, keyword, count, sdf.format(createDate))

        val _rdd = rdd.sparkContext.makeRDD(Seq(keywordCount))
        val cfg = Map(
          // 索引
          ConfigurationOptions.ES_RESOURCE -> "spark-streaming/kw-total",
          // 自定义id
          ConfigurationOptions.ES_MAPPING_ID -> "id",
          // 排除字段
          ConfigurationOptions.ES_MAPPING_EXCLUDE -> "id"
        )
        EsSpark.saveToEs(_rdd, cfg)
      })
      rdd.unpersist(true)
    })

    ssc.checkpoint(_checkpoint)
    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext
      .getOrCreate(_checkpoint, createContext _)
    ssc.start()
    ssc.awaitTermination()
  }

}
