package com.github.tiger.spark.streaming

import com.github.tiger.scala.json.JsonUtil
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 功能：职位统计
  *
  * 描述：借助流式计算，进行维度统计
  *
  * @author liuhongming
  */
object JobStreaming {

  case class Job(id: String, city_id: String, job_type: String, job_nature: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("Job Streaming")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.receiverStream(new JobReceiver())

    println(lines.count())

    // => Map[String, Any]
    val jsonData = lines.map(x => {
        JsonUtil.readValue(x, classOf[Map[String, Any]])
    })

    // => Job
    val jobMap = jsonData.map(data => (Job(data("ID").toString, data("CITY_ID").toString,
      data("JOB_TYPE").toString, data("JOB_NATURE").toString), 1))

    val jobRedcue = jobMap.reduceByKeyAndWindow((a: Int, b: Int) => (a + b),
      Seconds(20), Seconds(5))

    jobRedcue.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
