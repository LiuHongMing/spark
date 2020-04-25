package com.github.tiger.spark.streaming.realm

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.github.tiger.spark.util.{FastJsonUtil, Md5Util}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/**
  * 统计每天搜索关键字出现次数
  */
object SearchKeywordsDriver {

  lazy val logger = LoggerFactory.getLogger(SearchKeywordsDriver.getClass)

  val _checkpoint = "hdfs://spark-220/user/zpcampus/checkpoint"

  case class Keyword(timestamp: Long, keyword: String)

  case class KeywordCount(id: String, keyword: String, count: Long, createDate: String)

  case class Stateful(timestamp: Long = 0, keyword: String = "", count: Long = 0)

  def createContext(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName("Statistics[Search Keywords]")
      .setMaster("local[*]")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("es.nodes", "175.63.101.107,175.63.101.108,175.63.101.109")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")

    val secInterval = 30

    val ssc = new StreamingContext(conf, Seconds(secInterval))

    val topics = List("campus_search_condition")
    val kafkaParams = collection.Map[String, Object](
      "bootstrap.servers" -> "175.63.101.126:9092,175.63.101.128:9092,175.63.101.129:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group-campus_search_condition",
      "session.timeout.ms" -> "30000",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false",
      "max.poll.records" -> "100",
      "request.timeout.ms" -> "40000",
      "auto.commit.interval.ms" -> "1000"
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    /**
      * RDD 结构化转换操作
      */
    val messageStream = stream.transform(rdd => {
      rdd.map(record => {
        val jsonValue = record.value()
        val jsonObject = JSON.parseObject(jsonValue)
        jsonObject
      })
    }).map(jsonObject => {
      val writeTime = jsonObject.get("writeTime").toString
      val writeData = jsonObject.get("data").toString

      val (ok, result) = FastJsonUtil.tryConvert[AsMessage](writeData)
      var keyword = ""
      result match {
        case res if res.isInstanceOf[AsMessage] =>
          keyword = result.asInstanceOf[AsMessage].getKeyword
        case _ => println("None")
      }
      if (!ok || "".equals(keyword)) {
        Seq("0", "unknown")
      } else {
        Seq(writeTime, keyword)
      }
    }).filter(seq => {
      val writeTime = seq(0)
      java.lang.Long.valueOf(writeTime) > 0
    })

    /**
      * SQL 转换
      */
    val pairStream = messageStream.transform(rdd => {
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()

      import sparkSession.implicits._

      // 通过反射构建Schema
      val keywordsDF = rdd.map(fields => {
        val timestamp = new DateTime(fields(0).toLong).withHourOfDay(0).withMinuteOfHour(0)
          .withSecondOfMinute(0).withMillisOfSecond(0).getMillis
        val keyword = fields(1)
        Keyword(timestamp, keyword)
      }).toDF()

      keywordsDF.createOrReplaceTempView("keywords")

      val selectSql = s"select timestamp, keyword, count(keyword) as count" +
        s" from keywords group by timestamp, keyword"
      val selectResult = sparkSession.sql(selectSql)

      val _rdd = selectResult.rdd.map(row => {

        val timestamp = row.getAs[Long]("timestamp")
        val keyword = row.getAs[String]("keyword")
        val count = row.getAs[Long]("count")

        val key = Md5Util.md5(timestamp + "_" + keyword)
        val value = Stateful(timestamp, keyword, count)
        val stateful = (key, value)

        stateful
      })

      _rdd
    })

    /**
      * 开启 DStream 的 RDDs 周期性检查
      */
    pairStream.checkpoint(Seconds(5 * secInterval))

    val updateState = (newValues: Seq[Stateful], old: Option[Stateful]) => {
      val prev = old.getOrElse(Stateful())

      val newValueSum = newValues.flatMap(stateful => Seq(stateful.count)).sum
      val count = prev.count + newValueSum

      if (newValues.size > 0) {
        val curr = newValues.last
        Some(Stateful(curr.timestamp, curr.keyword, count))
      } else {
        Some(Stateful(prev.timestamp, prev.keyword, prev.count))
      }
    }

    val updateStateStream = pairStream.updateStateByKey(updateState)

    updateStateStream.print(100)

    updateStateStream.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        rdd.collect().foreach(pair => {
          val id = pair._1

          val timestamp = pair._2.timestamp
          val keyword = pair._2.keyword
          val count = pair._2.count
          val createDate = new Date(timestamp)

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
      }
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
