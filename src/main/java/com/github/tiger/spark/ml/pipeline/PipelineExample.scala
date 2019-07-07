package com.github.tiger.spark.ml.pipeline

import org.apache.spark.ml.Pipeline

object PipelineExample {

  def main(args: Array[String]): Unit = {

    val stages = Array()

    val pipeline = new Pipeline().setStages(stages)

  }

}
