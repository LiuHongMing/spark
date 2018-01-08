package com.github.tiger.scala.kafka

import java.util

import com.github.tiger.kafka.consumer.ConsumerHandler
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaCuratorScala {

}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {

    val topicList = List("zpcampus")

//    val recordOperator = new ConsumerRecordsFuture[ConsumerRecord[String, String]] {
//      override def commit(records: util.List[ConsumerRecord[String, String]]): Unit = {
//        nothing()
//      }
//
//      def nothing(): Unit = {
//
//      }
//
//      override def setOffset(partition: Int, offset: Int) = ???
//    }

//    ConsumerUtil.receive(topicList.asJava, recordOperator)

  }

}