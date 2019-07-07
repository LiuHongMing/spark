package com.github.tiger.spark.ml.onehotencoder

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object OneHotEncoderExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("OneHotEncoderExample").setMaster("local[8]")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // 将Seq集合转换成DataFrame
    // Seq是一个有先后次序的序列(也可以叫集合)，Vector Range List Array都属于Seq类型
    val df: DataFrame = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    // String => IndexDouble
    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex")
    val indexed = indexer.fit(df).transform(df)

    indexed.show()

    // OneHotEncode: 实际上是转换成了稀疏向量
    // Spark源码: The last category is not included by default 最后一个种类默认不包含
    // 和python scikit-learn's OneHotEncoder不同，scikit-learn's OneHotEncoder包含所有
    val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")
      // 设置最后一个是否包含
      .setDropLast(false)
    // transform 转换成稀疏向量
    val encoded = encoder.transform(indexed)
    encoded.select("category", "categoryIndex", "categoryVec").show()

    spark.stop()
  }

}
