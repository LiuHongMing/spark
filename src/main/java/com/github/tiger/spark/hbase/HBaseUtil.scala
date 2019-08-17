package com.github.tiger.spark.hbase

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory}
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

object HBaseUtil {

  def getTableDescriptor(tableName: String, columnFamily: String): HTableDescriptor = {
    val descriptor = new HTableDescriptor(TableName.valueOf(tableName))
    descriptor.addFamily(new HColumnDescriptor(columnFamily).setCompressionType(Algorithm.NONE))
    descriptor
  }

  @throws[IOException]
  def createOrOverwrite(admin: Admin, table: HTableDescriptor): Unit = {
    if (admin.tableExists(table.getTableName)) {
      admin.disableTable(table.getTableName)
      admin.deleteTable(table.getTableName)
    }
    admin.createTable(table)
  }

  def createTable(config: Configuration, tableName: String, columnFamily: String): Unit = {
    val connection = ConnectionFactory.createConnection(config)
    val admin = connection.getAdmin

    val tableDescriptor = getTableDescriptor(tableName, columnFamily)

    createOrOverwrite(admin, tableDescriptor)

    connection.close()
  }

  def deleteTable(config: Configuration, tableName: String): Unit = {

    val connection = ConnectionFactory.createConnection(config)
    val admin = connection.getAdmin

    val _tableName = TableName.valueOf(tableName)

    if (admin.tableExists(_tableName)) {
      admin.disableTable(_tableName)
      admin.deleteTable(_tableName)
    }

    connection.close()
  }


  def main(args: Array[String]): Unit = {

    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", "spark-220,spark-221,spark-223")

//    val cf = ""
//    createTable(hbaseConfig, "test_hbase_table", cf)

    deleteTable(hbaseConfig, "test_hbase_table")
  }
}
