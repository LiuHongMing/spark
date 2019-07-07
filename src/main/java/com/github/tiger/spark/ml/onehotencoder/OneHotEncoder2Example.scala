package com.github.tiger.spark.ml.onehotencoder

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{LabeledPoint, OneHotEncoder, StringIndexer}
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession

object OneHotEncoder2Example {

  case class Person(loss: Double, gender: String, age: Int, grade: String, region: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("OneHotEncoder2Example").setMaster("local[8]")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    // vector data
    val vectorData = spark.createDataFrame(
      Seq(
        (1.0, "male", 20, "advanced", "beijing"),
        (0.0, "female", 18, "junior", "shanghai"),
        (1.0, "male", 28, "advanced", "guangzhou")
      )
    ).toDF("loss", "gender", "age", "grade", "region")

    // index columns
    val stringColumns = Array("gender", "age", "grade", "region")
    val indexTransformers: Array[PipelineStage] = stringColumns.map(cname => {
      new StringIndexer().setInputCol(cname).setOutputCol(s"${cname}_index")
    })

    // index pipelines
    val indexPipeline = new Pipeline().setStages(indexTransformers)
    // estimator
    val indexModel = indexPipeline.fit(vectorData)
    // transformer
    val dfIndex = indexModel.transform(vectorData)
    dfIndex.show()

    // encode columns
    val indexColumns = dfIndex.columns.filter(index => index contains "index")
    // transformer
    val oneHotEncoders: Array[PipelineStage] = indexColumns.map(cname => {
      new OneHotEncoder().setInputCol(cname)
        .setOutputCol(s"${cname}_vec")
        .setDropLast(false)
    })

    val pipeline = new Pipeline().setStages(indexTransformers ++ oneHotEncoders)
    val model = pipeline.fit(vectorData)
    val dfModel = model.transform(vectorData)
      .select("loss",
        "gender_index_vec", "age_index_vec",
        "grade_index_vec", "region_index_vec")

    dfModel.show()

    dfModel.rdd.map(
      x =>
        LabeledPoint(
          x.getDouble(0),
          Vectors.dense(x.getAs[SparseVector]("gender_index_vec").toArray
            ++ x.getAs[SparseVector]("age_index_vec").toArray
            ++ x.getAs[SparseVector]("grade_index_vec").toArray
            ++ x.getAs[SparseVector]("region_index_vec").toArray)
        )
    ).foreach(lp => println(lp.toString()))

    spark.stop()
  }

}
