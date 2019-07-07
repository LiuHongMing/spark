package com.github.tiger.spark.streaming.realm

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.github.tiger.elastic.BasisCurdTransportClient
import com.github.tiger.scala.util.Md5Util
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}
import org.elasticsearch.common.xcontent.XContentFactory
import org.joda.time.DateTime

/**
  * 统计每天搜索关键字出现次数
  */
object SearchKeywordsDriver2 {

  val _checkpoint = "hdfs://spark-220/user/zpcampus/checkpoint"

  case class Keyword(timestamp: Long, keyword: String)

  case class KeywordCount(id: String, keyword: String, count: Long, createDate: String) {

    def toEsSource = {
      val source = XContentFactory.jsonBuilder()
        .startObject()
        .field("keyword", keyword)
        .field("count", count)
        .field("createDate", createDate)
        .endObject();
      source
    }

  }

  def createContext(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName("Statistics[Search Keywords]")
      .setMaster("local[*]")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("es.nodes", "175.63.101.107,175.63.101.108,175.63.101.109")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")

    val secInterval = 10

    val ssc = new StreamingContext(conf, Seconds(secInterval))

    val topics = List("campus_search_condition")
    val kafkaParams = collection.Map[String, Object](
      "bootstrap.servers" -> "175.63.101.126:9092,175.63.101.128:9092,175.63.101.129:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group-campus_search_condition",
      "session.timeout.ms" -> "30000",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false",
      "max.poll.records" -> "100",
      "request.timeout.ms" -> "40000",
      "auto.commit.interval.ms" -> "1000"
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val keywords = stream.map(record => {

      val jsonValue = record.value()
      val jsonObject = JSON.parseObject(jsonValue)

      val writeData = jsonObject.get("data").toString
      val (ok, result) = tryConvert[AsMessage](writeData)

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

      rdd.foreach(pair => {
        val timestamp = pair._1.timestamp
        val keyword = pair._1.keyword
        val count = pair._2
        val createDate = new Date(timestamp)

        val id = Md5Util.md5(timestamp + "_" + keyword)

        val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+0800'")
        val keywordCount = KeywordCount(id, keyword, count, sdf.format(createDate))

        val settings = Settings.builder()
          // 集群名称
          .put("cluster.name", "campus-logs")
          // 自动嗅探
          .put("client.transport.sniff", true)
          .build();

        // 集群地址
        val transportAddresses = Array(
          new InetSocketTransportAddress(
            InetAddress.getByName("175.63.101.107"), 9300).asInstanceOf[TransportAddress]
        )

        // TODO es连接池优化
        val client = BasisCurdTransportClient(settings, transportAddresses)
        client.saveDocument("spark-streaming", "kw-total", id, keywordCount.toEsSource)

      })

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
