package com.github.tiger.spark.streaming.structured

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class JobStructured {

  /**
    * 成员变量，用 _ 推导类型初始化
    */
  var id: String = _

}

object JobStructured extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("StructuredKafka")

  val spark = SparkSession.builder
    .config(conf).getOrCreate

  val schemaExp = StructType(
    StructField("ID", DataTypes.StringType, false)
      :: StructField("CITY_ID", DataTypes.StringType, true)
      :: StructField("JOB_TYPE", DataTypes.StringType, true)
      :: StructField("JOB_NATURE", DataTypes.StringType, true)
      :: Nil
  )

  val path = Thread.currentThread().getContextClassLoader.getResource("data").getPath
  val jobs = spark.readStream
    .schema(schemaExp)
    .format("json")
    .load(path)

  jobs.printSchema()

  import spark.implicits._

  val jobsCount = jobs.groupBy($"ID", $"CITY_ID")
    .agg(Map("JOB_TYPE" -> "max", "JOB_NATURE" -> "sum"))
    .orderBy("ID", "CITY_ID")

  val query = jobsCount.writeStream

    /**
      * append, complete, update
      */
    .outputMode("complete")

    /**
      * StreamWriter:
      *
      * parquet, foreach, console, memory
      */
    .format("console")

    /**
      * 设置定时器
      */
    .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
    .start()

  query.awaitTermination()

}