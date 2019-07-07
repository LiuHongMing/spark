package com.github.tiger.scala.lesson.basic

/**
  * 柯里化
  *
  * 在调用时，currying可以依次传入各个参数
  */
object Curried {

  def main(args: Array[String]): Unit = {
    println(sum(1)(2))
    println(sumCurring(1)(2))
  }

  // 柯里化方法
  def sum(m: Int)(n: Int) = m + n

  // 柯里化函数
  val sumCurring = sum _

}
