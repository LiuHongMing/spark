package com.github.tiger.scala.lang.basic

/**
  * 函数表达式说明
  */
object Functions extends App {

  // 带参数的匿名函数
  (x: Int) => x + 1

  // 命名函数
  val addOne = (x: Int) => x + 1

  // 多个参数
  val add = (x: Int, y: Int) => x + y

  // 无参数
  val getTheAnswer = () => 42

}
