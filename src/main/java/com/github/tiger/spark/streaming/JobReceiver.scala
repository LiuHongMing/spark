package com.github.tiger.spark.streaming

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class JobReceiver()
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_SER) with Logging {

  override def onStart(): Unit = {
    val worker = new Thread("Job Receiver") {
      override def run() = {
        receive()
      }
    }
    worker.start()

  }

  override def onStop(): Unit = {

  }

  private def receive(): Unit = {
    var readItem: String = null
    try {
      val in = new FileInputStream("/Users/liuhongming/" +
        "Documents/iCoding/workspace/java/spark/src/main/resources/job.json")
      val reader = new BufferedReader(new InputStreamReader(in, "UTF-8"))
      readItem = reader.readLine()

      while(!isStopped() && (readItem != null)) {
        store(readItem)
        readItem = reader.readLine()
        TimeUnit.SECONDS.sleep(1)
      }
      reader.close()
      restart("Try to connect again")
    } catch {
      case e: Exception =>
        restart("An exception occurred while receiving data", e)
    }
  }
}
