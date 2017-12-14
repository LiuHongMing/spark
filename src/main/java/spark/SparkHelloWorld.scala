package spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkHelloWorld {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("SparkHelloWorld")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val lines = sc.parallelize(Seq("hello world", "hello tencent"))
    val words = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    words.foreach(println)

    Thread.sleep(10 * 60 * 1000)
  }

}
