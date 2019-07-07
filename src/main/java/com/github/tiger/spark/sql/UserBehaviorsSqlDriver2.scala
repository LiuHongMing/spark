package com.github.tiger.spark.sql

import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.elasticsearch.common.xcontent.XContentFactory
import org.joda.time.DateTime

object UserBehaviorsSqlDriver2 {

  case class BehaviorRow(writeTime: Long, jobNum: String, favorite: Long, view: Long,
                         apply: Long, unfavorite: Long, timestamp: Timestamp)

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

  def hbase(conn: Connection, tableName: String)
           (rowKey: String, family: String, qualifier: String, value: String): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val put = new Put(rowKey.getBytes)
    put.addColumn(Bytes.toBytes(family),
      Bytes.toBytes(qualifier), Bytes.toBytes(value))
    table.put(put)
  }

  class BehaviorAvgUDAF extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(
      StructField("favorite", IntegerType) :: StructField("view", IntegerType)
        :: StructField("apply", IntegerType) :: StructField("unfavorite", IntegerType) :: Nil)

    override def bufferSchema: StructType = StructType(
      StructField("sum", IntegerType) :: Nil)

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getInt(0) + input.getInt(0)
      }
      if (!input.isNullAt(1)) {
        buffer(0) = buffer.getInt(0) + input.getInt(1)
      }
      if (!input.isNullAt(2)) {
        buffer(0) = buffer.getInt(0) + input.getInt(2)
      }
      if (!input.isNullAt(3)) {
        buffer(0) = buffer.getInt(0) + input.getInt(3)
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    }

    override def evaluate(buffer: Row): Any = buffer.getInt(0).toDouble / inputSchema.length
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Statistics[User Behavior]")

    val spark = SparkSession.builder
      .config(conf).getOrCreate

    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        "175.63.101.126:9092,175.63.101.128:9092,175.63.101.129:9092")
      .option("subscribe", "spark_user_behavior")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    df.printSchema()

    val messages = df.map(row => {

      new String(row.get(1).asInstanceOf[Array[Byte]])

    }).map(jsonValue => {

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
      val timestamp = new Timestamp(writeTimeMillis)

      // TODO 先存储jobNum, 后过滤jobNum
      val rowKey = jobNum
      val family = "job_num"
      val qualifier = jobNum
      val value = "new"

      //      HBaseReadWriter.writeData("user_behavior_jobs", rowKey, family, qualifier, value)

      BehaviorRow(writeTimeMillis, jobNum, favorite, view, apply, unfavorite, timestamp)

    })

    messages.createOrReplaceTempView("messages")

    val start = DateTime.now().withHourOfDay(0).withMinuteOfHour(0)
      .withSecondOfMinute(0).withMillisOfSecond(0).getMillis

    /**
      * 自定义函数
      */
    //    val behaviorUDF = (favorite: Int, view: Int) => {
    //      favorite + view
    //    }

    /**
      * 自定义聚合函数
      */
    spark.sqlContext.udf.register("behaviorAvg", new BehaviorAvgUDAF)

    val jobBehaviorAgg = spark.sql(
      "select jobNum, sum(favorite) as favorite, " +
        "sum(view) as view, sum(apply) as apply, sum(unfavorite) as unfavorite, " +
        "behaviorAvg(favorite, view, apply, unfavorite) as behaviorAvg " +
        s"from messages where writeTime = $start group by jobNum")
    jobBehaviorAgg.writeStream.outputMode("complete").format("console").start()

    spark.streams.awaitAnyTermination()

  }
}
