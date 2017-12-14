package scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ScalaProgram {

  def main(args: Array[String]) {
    val map = Map("111 222" -> "333 444", ("aaa", "bbb"))
    println(map)
    println(map.map(_._2.split(" ")))
    //    println(map.flatMap(_._2.split(" ")))
    //    println(map.flatMap(_._2.split(" ")).map((_, 1)))
    //
    //    val topics = "log"
    //    val numThreads = "1"
    //    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //    println(topicMap)

//    (1 to 9).reduceLeft(_ * _)
//
//    "Marry has a little lamb".split(" ").sortWith(_.length < _.length)
//
//    runInThread {
//      println("Hi");
//      Thread.sleep(10000);
//      println("Bye")
//    } {
//      println("Yes");
//    }
//
//    var x = 10
//    until(x == 0) {
//      x -= 1
//      println(x)
//    }
//
//  }
//
//  def runInThread(block: => Unit)(block2: => Unit) {
//    new Thread() {
//      override def run(): Unit = {
//        block
//      }
//    }.start()
//
//    block2
//  }
//
//  def until(condition: => Boolean)(block: => Unit) {
//    if (!condition) {
//      block
//      until(condition)(block)
//    }
  }
}
