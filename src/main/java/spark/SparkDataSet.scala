package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class Person(name: String, age: Long)

object SparkDataSet {

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

    // DataSet
    val caseClassDS = Seq(Person("Andy", 23)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect()
    primitiveDS.show()

    val sample = cp + "people.json"
    val peopleDS = spark.read.json(sample).as[Person]
    peopleDS.show()
  }

}
