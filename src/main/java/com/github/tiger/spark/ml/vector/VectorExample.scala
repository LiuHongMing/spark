package com.github.tiger.spark.ml.vector

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}

object VectorExample {

  def main(args: Array[String]): Unit = {

    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))

    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)

    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    println(pos.toString())
  }

}
