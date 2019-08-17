package com.github.tiger.spark.hbase

import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka010.OffsetRange

object KafkaOffsetManager {

  def saveOffsets(TOPIC_NAME: String, GROUP_ID: String, offsetRanges: Array[OffsetRange],
                  hbaseTableName: String, batchTime: org.apache.spark.streaming.Time) = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "spark-220,spark-221,spark-223")

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))

    val rowKey = TOPIC_NAME + ":" + GROUP_ID + ":" + String.valueOf(batchTime.milliseconds)
    val put = new Put(rowKey.getBytes)
    for (offset <- offsetRanges) {
      put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes(offset.partition.toString),
        Bytes.toBytes(offset.untilOffset.toString))
    }
    table.put(put)
    conn.close()
  }

  def getLastCommittedOffsets(TOPIC_NAME: String, GROUP_ID: String,
                              hbaseTableName: String, zkQuorum: String, zkRootDir: String,
                              sessionTimeout: Int, connectionTimeOut: Int): Map[TopicPartition, Long] = {
    val zkUrl = zkQuorum + "/" + zkRootDir
    val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeOut)
    val zkClient: ZkClient = zkClientAndConnection._1
    val zkConnection: ZkConnection = zkClientAndConnection._2

    val zkUtils = new ZkUtils(zkClient, zkConnection, false)
    val zKNumberOfPartitionsForTopic =
      zkUtils.getPartitionsForTopics(Seq(TOPIC_NAME)).get(TOPIC_NAME).toList.head.size

    zkClient.close()
    zkConnection.close()

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "spark-220,spark-221,spark-223")
    // Connect to HBase to retrieve last committed offsets
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val startRow = TOPIC_NAME + ":" + GROUP_ID + ":" + String.valueOf(System.currentTimeMillis())
    val stopRow = TOPIC_NAME + ":" + GROUP_ID + ":" + 0
    val scan = new Scan()
    val scanner = table.getScanner(scan.setStartRow(startRow.getBytes).setStopRow(
      stopRow.getBytes).setReversed(true))
    val result = scanner.next()
    // Set the number of partitions discovered for a topic in HBase to 0
    var hbaseNumberOfPartitionsForTopic = 0
    if (result != null) {
      hbaseNumberOfPartitionsForTopic = result.listCells().size()
    }

    val fromOffsets = collection.mutable.Map[TopicPartition, Long]()

    if (hbaseNumberOfPartitionsForTopic == 0) {
      // initialize fromOffsets to beginning
      for (partition <- 0 to zKNumberOfPartitionsForTopic - 1) {
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> 0)
      }
    } else if (zKNumberOfPartitionsForTopic > hbaseNumberOfPartitionsForTopic) {
      // handle scenario where new partitions have been added to existing kafka topic
      for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {
        val fromOffset = Bytes.toString(result.getValue(Bytes.toBytes("offsets"),
          Bytes.toBytes(partition.toString)))
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> fromOffset.toLong)
      }
      for (partition <- hbaseNumberOfPartitionsForTopic to zKNumberOfPartitionsForTopic - 1) {
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> 0)
      }
    } else {
      // initialize fromOffsets from last run
      for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {
        val fromOffset = Bytes.toString(result.getValue(Bytes.toBytes("offsets"),
          Bytes.toBytes(partition.toString)))
        fromOffsets += (new TopicPartition(TOPIC_NAME, partition) -> fromOffset.toLong)
      }
    }
    scanner.close()
    conn.close()
    fromOffsets.toMap
  }

  def main(args: Array[String]): Unit = {
    val TOPIC_NAME = "campus_search_condition"

    val offsetRange = Array(
      OffsetRange(TOPIC_NAME, 0, 0, 500),
      OffsetRange(TOPIC_NAME, 1, 0, 200),
      OffsetRange(TOPIC_NAME, 2, 0, 300)
    )

    val GROUP_ID = "group-campus_search_condition"
    val tableName = "stream_kafka_offsets"
    val batchTime = "1497628830000".toLong

    saveOffsets(TOPIC_NAME, GROUP_ID, offsetRange, tableName, Time(batchTime))

    val zkQuorum = "175.63.101.107:2181"
    val zkRootDir = ""

    val sessionTimeout = 10000
    val connectionTimeOut = 10000

    val fromOffsets = getLastCommittedOffsets(TOPIC_NAME, GROUP_ID,
      tableName, zkQuorum, zkRootDir, sessionTimeout, connectionTimeOut)

    println(fromOffsets)
  }

}
