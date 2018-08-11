package com.github.tiger.spark.demo

import com.github.tiger.scala.json.JsonUtil
import org.apache.spark.{SparkConf, SparkContext}

object MultiDimension {

  def main(args: Array[String]): Unit = {

    case class Job(ID: String, CITY_ID: String, JOB_TYPE: String, JOB_NATURE: String)

    val conf = new SparkConf().setAppName("Demo").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val data = List(
      """{"ID": "CC000882586J90000041000", "CITY_ID": "538", "JOB_TYPE": "148", "JOB_NATURE": "2"}""",
      """{"ID": "CC000882586J90000041000", "CITY_ID": "538", "JOB_TYPE": "148", "JOB_NATURE": "2"}""",
      """{"ID": "CC000882586J90000041000", "CITY_ID": "538", "JOB_TYPE": "148", "JOB_NATURE": "2"}""",
      """{"ID": "CC000131707J90000000000", "CITY_ID": "530", "JOB_TYPE": "130", "JOB_NATURE": "2"}""")

    val distData = sc.parallelize(data)

    val groupData = distData.map(line => {

      val jobMap = JsonUtil.readValue(line, classOf[Map[String, String]])

      val id = jobMap.get("ID").get
      val cityId = jobMap.get("CITY_ID").get
      val jobType = jobMap.get("JOB_TYPE").get
      val jobNature = jobMap.get("JOB_NATURE").get

      (id, Job(id, cityId, jobType, jobNature))

    }).groupByKey().map((f: (String, Iterable[Job])) => {

      var seq = scala.collection.mutable.Seq[(String, String)]()
      for (job <- f._2) {
        seq = seq :+ (f._1.toString, job.CITY_ID)
      }
      seq

    }).flatMap((f) => {
      f
    }).map((f: (String, String)) => (f, 1))
      .reduceByKey((a, b) => a + b)
      .foreach((f: ((String, String), Int)) => {

        val id = f._1._1
        val cityId = f._1._2

        println(String.format("key:%s", id +"," + cityId), "count:" + f._2)
      })


  }

}
