package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkDataFrame {

  def main(args: Array[String]) {
    val cp = Thread.currentThread().getContextClassLoader.getResource("").getPath

    val conf = new SparkConf()
    conf.setAppName("Spark SQL basic example").setMaster("local[*]")
    conf.set("spark.some.config.option", "some-value")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // scala语法
    import spark.implicits._

    val sample = cp + "people.json"
    val df = spark.read.json(sample)
    df.show()
    df.printSchema()

    df.select($"name", $"age" + 1).show()

    df.filter($"age" > 21).show()

    df.groupBy("age").count().show()

    Thread.sleep(60 * 10 * 1000)
  }

}
