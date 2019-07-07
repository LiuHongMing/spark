package com.github.tiger.spark.streaming.realm

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}
import org.elasticsearch.common.xcontent.XContentFactory
import org.joda.time.DateTime

object UserBehaviorsDriverRelease {

  case class BehaviorState(timestamp: Long, jobNum: String, favorite: Long = 0,
                           view: Long = 0, apply: Long = 0, unfavorite: Long = 0)

  case class BehaviorEs(jobNum: String, favorite: Long, view: Long, apply: Long,
                        unfavorite: Long, createDate: String) {
    def toEsSource = {
      val source = XContentFactory.jsonBuilder()
        .startObject()
        .field("jobNum", jobNum)
        .field("favorite", favorite)
        .field("view", view)
        .field("apply", apply)
        .field("unfavorite", unfavorite)
        .field("createDate", createDate)
        .endObject();
      source
    }
  }

  def createContext(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName(s"Statistics[User Behavior]")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("es.nodes", "172.30.5.17,172.30.5.18,172.30.5.19")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")

    val secInterval = 10

    val ssc = new StreamingContext(conf, Seconds(secInterval))

    val topics = List("spark_user_behavior")
    val kafkaParams = collection.Map[String, Object](
      "bootstrap.servers" -> "172.30.100.90:9092,172.30.100.91:9092,172.30.100.92:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> s"group-spark_user_behavior",
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

    val messages = stream.map(record => {

      val jsonValue = record.value()
      val jsonObject = JSON.parseObject(jsonValue)

      val writeData = jsonObject.get("data").toString

      val (_, result) = FastJsonUtil.tryConvert[UserBehaviors](writeData)

      var jobNum = ""
      var optType = ""

      result match {
        case res if res.isInstanceOf[UserBehaviors] => {
          jobNum = result.asInstanceOf[UserBehaviors].getJobNum
          optType = result.asInstanceOf[UserBehaviors].getOpType
        }
        case _ => println("None")
      }
      val writeTime = jsonObject.get("writeTime").toString.toLong
      val writeTimeMillis = new DateTime(writeTime)
        .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
        .withMillisOfSecond(0).getMillis

      val str2Int = (optType: String, matchString: String) => {
        if (matchString.equals(optType)) {
          1
        } else {
          0
        }
      }

      val favorite = str2Int(optType, "favorite")
      val view = str2Int(optType, "view")
      val apply = str2Int(optType, "apply")
      val unfavorite = str2Int(optType, "unfavorite")

      val id = Md5Util.md5(jobNum + writeTimeMillis)

      (id, BehaviorState(writeTimeMillis, jobNum, favorite, view, apply, unfavorite))

    })

    val filterMessages = messages.filter(pair => {
      val jobNum = pair._1
      val start = DateTime.now().withHourOfDay(0).withMinuteOfHour(0)
        .withSecondOfMinute(0).withMillisOfSecond(0).getMillis
      val timestamp = pair._2.timestamp

      if (start == timestamp && !(jobNum == null || jobNum.isEmpty)) {
        true
      } else {
        false
      }
    })

    def mappingFunc(key: String, value: Option[BehaviorState],
                    state: State[BehaviorState]): (String, BehaviorState) = {
      val oldState = state.getOption().getOrElse(BehaviorState(0, "", 0, 0, 0, 0))

      val favorite = oldState.favorite + value.get.favorite
      val view = oldState.view + value.get.view
      val apply = oldState.apply + value.get.apply
      val unfavorite = oldState.unfavorite + value.get.unfavorite
      val timestamp = value.get.timestamp
      val jobNum = value.get.jobNum

      val newState = BehaviorState(timestamp, jobNum, favorite, view, apply, unfavorite)
      state.update(newState)
      (key, newState)
    }

    val stateSpec = StateSpec.function[String, BehaviorState,
      BehaviorState, (String, BehaviorState)](mappingFunc _)

    val userBehaviors: DStream[(String, BehaviorState)] =
      filterMessages.mapWithState[BehaviorState, (String, BehaviorState)](stateSpec)

    userBehaviors.foreachRDD(rdd => {

      rdd.foreach(pair => {
        val id = pair._1

        val jobNum = pair._2.jobNum

        val timestamp = pair._2.timestamp
        val favorite = pair._2.favorite
        val view = pair._2.view
        val apply = pair._2.apply
        val unfavorite = pair._2.unfavorite

        val createDate = new Date(timestamp)

        val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+0800'")
        val behaviorEs = BehaviorEs(jobNum, favorite, view, apply,
          unfavorite, sdf.format(createDate))

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
        client.saveDocument("spark-streaming", "user-behavior-job-total", id, behaviorEs.toEsSource)

      })

    })

    ssc
  }

  def main(args: Array[String]): Unit = {

    val _checkpoint = "hdfs://master/user/zpcampus/user/behavior/job/checkpoint"
    val _context = createContext _

    val ssc = StreamingContext.getOrCreate(_checkpoint, _context)
    ssc.checkpoint(_checkpoint)

    ssc.start()
    ssc.awaitTermination()
  }

}
