package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL {

  def main(args: Array[String]) {
    val cp = Thread.currentThread().getContextClassLoader.getResource("").getPath

    val conf = new SparkConf()
    conf.setAppName("Spark SQL basic example").setMaster("local[*]")
    conf.set("spark.some.config.option", "some-value")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // DataFrame
    val sample = cp + "people.json"
    val df = spark.read.json(sample)

    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("select * from people")
    sqlDF.show()

  }

}
