package com.github.tiger.scala.basic

/**
  * 柯里化函数
  */
object Curry {

  def main(args: Array[String]): Unit = {
    println(sumCurried(1)(2))
  }

  val sum: (Int, Int) => Int = _ + _

  val sumCurried: Int => Int => Int = sum.curried

}
