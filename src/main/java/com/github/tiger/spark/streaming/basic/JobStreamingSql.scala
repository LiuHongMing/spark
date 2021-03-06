package com.github.tiger.spark.streaming.basic

import com.github.tiger.spark.util.SparkSessionSingleton
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * 功能：职位统计
  * 描述：借助流式计算，进行维度统计
  *
  * @author liuhongming
  */
object JobStreamingSql {

  case class Job(id: String, city_id: String, job_type: String, job_nature: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Job Streaming")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.receiverStream(new JobReceiver()).window(Seconds(15))

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      if (rdd.count() > 0) {
        val spark = SparkSessionSingleton.getInstance(conf)
        import spark.implicits._

        val jsonDataFrame = spark.read.json(rdd.toDS())

        jsonDataFrame.createOrReplaceTempView("job")

        val sqlDataFrame = spark.sql("select id, job_type, city_id, job_nature, count(1) cnt " +
          "from job group by id, job_type, city_id, job_nature")

        println(s"========= $time =========")
        sqlDataFrame.show()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}