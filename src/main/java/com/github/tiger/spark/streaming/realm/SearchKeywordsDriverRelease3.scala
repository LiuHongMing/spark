package com.github.tiger.spark.streaming.realm

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.github.tiger.spark.elastic.BasisCurdTransportClient
import com.github.tiger.spark.util.{FastJsonUtil, Md5Util}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}
import org.elasticsearch.common.xcontent.XContentFactory
import org.joda.time.DateTime

/**
  * 统计每天搜索关键字出现次数
  */
object SearchKeywordsDriverRelease3 {

  val _checkpoint = "hdfs://master/user/zpcampus/checkpoint"

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

    val messages = stream.map(record => {

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

      (s"${writeTimeMillis}:${keyword}", 1)

    }).filter(kw => {
      val start = DateTime.now().withHourOfDay(0).withMinuteOfHour(0)
        .withSecondOfMinute(0).withMillisOfSecond(0).getMillis
      val tokens = kw._1.split(":")
      if (start.toString.equals(tokens(0))) {
        true
      } else {
        false
      }
    })

    def mappingFunc(key: String, value: Option[Int],
                    state: State[Long]): (String, Long) = {
      val oldState = state.getOption().getOrElse(0L)
      val newState = oldState + value.getOrElse(0)
      state.update(newState)
      (key, newState)
    }

    val stateSpec = StateSpec.function[String, Int,
      Long, (String, Long)](mappingFunc _)

    val keywords: DStream[(String, Long)] = messages.mapWithState[Long, (String, Long)](stateSpec)

    keywords.foreachRDD(rdd => {

      rdd.foreach(pair => {
        val tokens = pair._1.split(":")
        val timestamp = tokens(0).toLong
        val keyword = tokens(1)
        val count = pair._2
        val createDate = new Date(timestamp)

        val id = Md5Util.md5(timestamp + "_" + keyword)

        val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+0800'")
        val keywordCount = KeywordCount(id, keyword, count, sdf.format(createDate))

        val settings = Settings.builder()
          // 集群名称
          .put("cluster.name", "logstash_ela")
          // 自动嗅探
          .put("client.transport.sniff", true)
          .build();

        // 集群地址
        val transportAddresses = Array(
          new InetSocketTransportAddress(
            InetAddress.getByName("172.30.5.17"), 9300).asInstanceOf[TransportAddress],
          new InetSocketTransportAddress(
            InetAddress.getByName("172.30.5.18"), 9300).asInstanceOf[TransportAddress],
          new InetSocketTransportAddress(
            InetAddress.getByName("172.30.5.19"), 9300).asInstanceOf[TransportAddress]
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
