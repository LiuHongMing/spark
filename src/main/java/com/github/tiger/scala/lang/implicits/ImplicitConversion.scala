package com.github.tiger.scala.lang.implicits

/**
  * 隐式视图：
  *
  * 是指把一种类型自动转换到另外一种类型，以符合表达式的要求。
  *
  * 隐式类型转换：
  * 是编译器发现传递的数据类型与申明不一致时，编译器在当前作用域查找类型转换方法，对数据类型进行转换。
  *
  * 隐式方法调用：
  * 是当编译器发现一个对象存在未定义的方法调用时，就会在当前作用域中查找是否存在对该对象的类型隐式转换，
  * 如果有，就查找转换后的对象是否存在该方法，存在，则调用。
  */
object ImplicitConversion extends App {

  /**
    * 隐式类型转换
    */
  implicit def strToInt(x: String) = x.toInt

  val y: Int = "123"
  println(y)

  /**
    * 隐式方法调用
    */
  class Hadoop {
    def compute(s: String) = {
      println(s"$s compute")
    }
  }

  class Spark {}

  object mapreduce {
    implicit def job(s: Spark) = new Hadoop()
  }

  import mapreduce._

  val spark = new Spark()
  spark.compute("Streaming")
}
