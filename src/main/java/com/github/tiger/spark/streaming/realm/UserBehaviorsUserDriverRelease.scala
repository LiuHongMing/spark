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
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}
import org.elasticsearch.common.xcontent.XContentFactory
import org.joda.time.DateTime

import scala.collection.mutable

object UserBehaviorsUserDriverRelease {

  case class BehaviorState(timestamp: Long,
                           campusId: String, campusGUID: String,
                           tagSubType: mutable.Map[String, mutable.Map[String, Int]],
                           jobType: mutable.Map[String, mutable.Map[String, Int]],
                           cityId: mutable.Map[String, mutable.Map[String, Int]],
                           companySize: mutable.Map[String, mutable.Map[String, Int]],
                           companyType: mutable.Map[String, mutable.Map[String, Int]],
                           opType: String)

  case class BehaviorEs(campusId: String, campusGUID: String,
                        tagSubType: mutable.Map[String, mutable.Map[String, Int]],
                        jobType: mutable.Map[String, mutable.Map[String, Int]],
                        cityId: mutable.Map[String, mutable.Map[String, Int]],
                        companySize: mutable.Map[String, mutable.Map[String, Int]],
                        companyType: mutable.Map[String, mutable.Map[String, Int]],
                        createDate: String,
                        lastUpdateTime: String) {

    val map2Json = (map: mutable.Map[String, Int]) => {
      map.map(m => s""""${m._1}":${m._2}""").mkString("{", ",", "}")
    }

    def toEsSource = {

      val tagSubTypeJson = tagSubType
        .map(m => s""""${m._1}":${map2Json(m._2)}""").mkString("{", ",", "}")
      val jobTypeJson = jobType
        .map(m => s""""${m._1}":${map2Json(m._2)}""").mkString("{", ",", "}")
      val cityIdJson = cityId
        .map(m => s""""${m._1}":${map2Json(m._2)}""").mkString("{", ",", "}")
      val companySizeJson = companySize
        .map(m => s""""${m._1}":${map2Json(m._2)}""").mkString("{", ",", "}")
      val companyTypeJson = companyType
        .map(m => s""""${m._1}":${map2Json(m._2)}""").mkString("{", ",", "}")

      val source = XContentFactory.jsonBuilder()
        .startObject()
        .field("campusId", campusId)
        .field("campusGUID", campusGUID)
        .field("tagSubType", tagSubTypeJson)
        .field("jobType", jobTypeJson)
        .field("cityId", cityIdJson)
        .field("companySize", companySizeJson)
        .field("companyType", companyTypeJson)
        .field("createDate", createDate)
        .field("lastUpdateTime", lastUpdateTime)
        .endObject();
      source
    }
  }

  def createContext(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName(s"Statistics[User Behavior -> User]")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.yarn.am.waitTime", "1000s")
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
      "group.id" -> s"group-spark_user_behavior_user",
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

      var campusId: String = null
      var campusGUID: String = null
      var opType: String = null
      var tagSubType: Array[String] = null
      var jobType: Array[String] = null
      var cityId: String = null
      var companySize: String = null
      var companyType: String = null

      result match {
        case res if res.isInstanceOf[UserBehaviors] => {
          val userBehaviors = result.asInstanceOf[UserBehaviors]
          campusId = userBehaviors.getCampusId
          campusGUID = userBehaviors.getCampusGUID
          opType = userBehaviors.getOpType
          tagSubType = userBehaviors.getTagSubType
          jobType = userBehaviors.getJobType
          cityId = userBehaviors.getCityId
          companySize = userBehaviors.getCompanySize
          companyType = userBehaviors.getCompanyType
        }
        case _ => println("None")
      }
      val writeTime = jsonObject.get("writeTime").toString.toLong
      var writeTimeMillis = new DateTime(writeTime)
        .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
        .withMillisOfSecond(0).getMillis

      val id = Md5Util.md5(campusId + campusGUID + writeTimeMillis)

      val createMutableMap = (opType: String, value: Seq[String]) => {
        val all = mutable.Map[String, mutable.Map[String, Int]]()

        val map = mutable.Map[String, Int]()
        value.foreach(tag => map.put(tag, 1))

        all.put(opType, map)

        all
      }

      if (tagSubType == null || jobType == null || cityId == null
        || companySize == null || companyType == null) {
        writeTimeMillis = 0
        tagSubType = Array[String]()
        jobType = Array[String]()
        cityId = ""
        companySize = ""
        companyType = ""
      }

      // tagSubType
      val tagSubTypeMap = createMutableMap(opType, tagSubType)

      // jobType
      val jobTypeMap = createMutableMap(opType, jobType)

      // cityId
      val cityIdMap = createMutableMap(opType, Seq(cityId))

      // companySize
      val companySizeMap = createMutableMap(opType, Seq(companySize))

      // companyType
      val companyTypeMap = createMutableMap(opType, Seq(companyType))

      (id, BehaviorState(writeTimeMillis, campusId, campusGUID,
        tagSubTypeMap, jobTypeMap, cityIdMap, companySizeMap,
        companyTypeMap, opType))

    }).filter(pair => {
      val start = DateTime.now().withHourOfDay(0).withMinuteOfHour(0)
        .withSecondOfMinute(0).withMillisOfSecond(0).getMillis
      val timestamp = pair._2.timestamp
      if (start == timestamp) {
        true
      } else {
        false
      }
    })

    val mappingFunc = (key: String,
                       value: Option[BehaviorState],
                       state: State[BehaviorState]) => {
      if (state.isTimingOut()) {
        println(s"$key is timing out")
      } else {
        val oldState = state.getOption().getOrElse(BehaviorState(0, "", "",
          mutable.Map[String, mutable.Map[String, Int]](),
          mutable.Map[String, mutable.Map[String, Int]](),
          mutable.Map[String, mutable.Map[String, Int]](),
          mutable.Map[String, mutable.Map[String, Int]](),
          mutable.Map[String, mutable.Map[String, Int]](), ""))

        val currState = value.getOrElse(BehaviorState(0, "", "",
          mutable.Map[String, mutable.Map[String, Int]](),
          mutable.Map[String, mutable.Map[String, Int]](),
          mutable.Map[String, mutable.Map[String, Int]](),
          mutable.Map[String, mutable.Map[String, Int]](),
          mutable.Map[String, mutable.Map[String, Int]](), ""))

        val opType = currState.opType
        if ("".equals(opType)) {

          (key, state)

        } else {

          val merge = (oldMap: mutable.Map[String, Int],
                       newMap: mutable.Map[String, Int]) => {
            oldMap ++ newMap.map(t => t._1 -> (t._2 + oldMap.getOrElse(t._1, 0)))
          }

          val tagSubTypeMap = oldState.tagSubType
          val tagSubType = merge(tagSubTypeMap.get(opType).getOrElse(mutable.Map[String, Int]()),
            currState.tagSubType.get(opType).getOrElse(mutable.Map[String, Int]()))
          tagSubTypeMap.put(opType, tagSubType)

          val jobTypeMap = oldState.jobType
          val jobType = merge(jobTypeMap.get(opType).getOrElse(mutable.Map[String, Int]()),
            currState.jobType.get(opType).getOrElse(mutable.Map[String, Int]()))
          jobTypeMap.put(opType, jobType)

          val cityIdMap = oldState.cityId
          val cityId = merge(cityIdMap.get(opType).getOrElse(mutable.Map[String, Int]()),
            currState.cityId.get(opType).getOrElse(mutable.Map[String, Int]()))
          cityIdMap.put(opType, cityId)

          val companySizeMap = oldState.companySize
          val companySize = merge(companySizeMap.get(opType).getOrElse(mutable.Map[String, Int]()),
            currState.companySize.get(opType).getOrElse(mutable.Map[String, Int]()))
          companySizeMap.put(opType, companySize)

          val companyTypeMap = oldState.companyType
          val companyType = merge(companyTypeMap.get(opType).getOrElse(mutable.Map[String, Int]()),
            currState.companyType.get(opType).getOrElse(mutable.Map[String, Int]()))
          companyTypeMap.put(opType, companyType)

          val newState = BehaviorState(currState.timestamp,
            currState.campusId, currState.campusGUID,
            tagSubTypeMap, jobTypeMap, cityIdMap, companySizeMap, companyTypeMap, opType)
          state.update(newState)

          (key, newState)
        }
      }
    }

    val stateSpec = StateSpec.function(mappingFunc).timeout(Minutes(6 * 60))

    val userBehaviors = messages.mapWithState(stateSpec)

    userBehaviors.stateSnapshots().foreachRDD(rdd => {

      rdd.foreach(pair => {

        val id = pair._1

        val timestamp = pair._2.timestamp
        val campusId = pair._2.campusId
        val campusGUID = pair._2.campusGUID
        val tagSubType = pair._2.tagSubType
        val jobType = pair._2.jobType
        val cityId = pair._2.cityId
        val companySize = pair._2.companySize
        val companyType = pair._2.companyType

        val createDate = new Date(timestamp)
        val lastUpdateTime = new Date()

        val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+0800'")

        val behaviorEs = BehaviorEs(campusId, campusGUID,
          tagSubType, jobType, cityId, companySize, companyType,
          sdf.format(createDate), sdf.format(lastUpdateTime))

        val settings = Settings.builder()
          // 集群名称
          .put("cluster.name", "logstash_ela")
          // 自动嗅探
          .put("client.transport.sniff", true)
          .build();

        // 集群地址
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
        client.saveDocument("spark-streaming", "user-behavior-user-total", id, behaviorEs.toEsSource)

      })
    })

    ssc
  }

  def main(args: Array[String]): Unit = {

    val _checkpoint = "hdfs://master/user/zpcampus/user/behavior/user/checkpoint"
    val _context = createContext _

    val ssc = StreamingContext.getOrCreate(_checkpoint, _context)
    ssc.checkpoint(_checkpoint)

    ssc.start()
    ssc.awaitTermination()
  }

}
