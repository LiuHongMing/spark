package com.github.tiger.spark.streaming.realm

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}
import org.elasticsearch.common.xcontent.XContentFactory
import org.joda.time.DateTime

import scala.util.matching.Regex

/**
  * 统计每天搜索字段出现次数
  */
object SearchFieldsDriverRelease2 {

  /**
    * CITY_ID         工作地点
    * JOB_NATURE      职位类型
    * JOB_TYPE        职位小类
    * INDUSTRY_ID     行业类型
    * COMPANY_TYPE    公司性质
    */

  case class SearchFieldCount(searchField: String, searchValue: String,
                              count: Long, createDate: String) {

    def toEsSource = {
      val source = XContentFactory.jsonBuilder()
        .startObject()
        .field("searchField", searchField)
        .field("searchValue", searchValue)
        .field("count", count)
        .field("createDate", createDate)
        .endObject();
      source
    }

  }

  def createContext(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName(s"Statistics[Search SearchField]")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("es.nodes", "172.30.5.17,172.30.5.18,172.30.5.19")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")

    val secInterval = 10

    val ssc = new StreamingContext(conf, Seconds(secInterval))

    val topics = List("campus_search_condition")
    val kafkaParams = collection.Map[String, Object](
      "bootstrap.servers" -> "172.30.100.90:9092,172.30.100.91:9092,172.30.100.92:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> s"group-search_field-campus_search_condition",
      "session.timeout.ms" -> "30000",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "true",
      "max.poll.records" -> "100",
      "request.timeout.ms" -> "40000",
      "auto.commit.interval.ms" -> "1000"
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    val pattern = new Regex(s"(CITY_ID|JOB_NATURE|JOB_TYPE|INDUSTRY_ID|COMPANY_TYPE):([0-9]+)")

    val messages = stream.map(record => {

      val jsonValue = record.value()
      val jsonObject = JSON.parseObject(jsonValue)

      val writeData = jsonObject.get("data").toString
      val (_, result) = tryConvert[AsMessage](writeData)

      var conditions = Array[String]()
      result match {
        case res if res.isInstanceOf[AsMessage] =>
          conditions = result.asInstanceOf[AsMessage].getCondition
        case _ => println("None")
      }

      val writeTime = jsonObject.get("writeTime").toString.toLong

      val fieldValueSeq = conditions.flatMap(condition => {
        var res = Seq[String]()
        (pattern findAllIn condition).matchData foreach (
          m => {
            res :+= m.group(0)
          })
        res
      })

      fieldValueSeq.map(fieldValue => {

        val writeTimeMillis = new DateTime(writeTime)
          .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
          .withMillisOfSecond(0).getMillis

        s"${writeTimeMillis}:${fieldValue}"

      }).toSeq

    }).flatMap(s => s.iterator)
      .map(key => (key, 1))
      .filter(pair => {
        val start = DateTime.now().withHourOfDay(0).withMinuteOfHour(0)
          .withSecondOfMinute(0).withMillisOfSecond(0).getMillis
        val tokens = pair._1.split(":")
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

    val searchFields: DStream[(String, Long)] = messages.mapWithState[Long, (String, Long)](stateSpec)

    searchFields.foreachRDD(rdd => {

      rdd.foreach(pair => {
        val tokens = pair._1.split(":")

        val timestamp = tokens(0).toLong
        val searchField = tokens(1).toLowerCase
        val searchValue = tokens(2)

        val count = pair._2
        val createDate = new Date(timestamp)

        val id = Md5Util.md5(timestamp + "_" + searchField + "_" + searchValue)

        val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+0800'")
        val searchFieldCount = SearchFieldCount(searchField, searchValue,
          count, sdf.format(createDate))

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

        val client = BasisCurdTransportClient(settings, transportAddresses)
        client.saveDocument("spark-streaming", "search-field-total", id, searchFieldCount.toEsSource)

      })

    })

    ssc
  }

  def main(args: Array[String]): Unit = {

    val _checkpoint = s"hdfs://master/user/zpcampus/search/field/checkpoint"
    val _context = createContext _

    val ssc = StreamingContext.getOrCreate(_checkpoint, _context)
    ssc.checkpoint(_checkpoint)

    ssc.start()
    ssc.awaitTermination()
  }

}
