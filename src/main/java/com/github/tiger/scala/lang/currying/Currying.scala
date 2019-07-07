package com.github.tiger.scala.lang.currying

/**
  * 方法可以定义多个参数列表。
  * 当使用较少数量的参数列表调用方法时，这将产生一个函数，将缺少的参数列表作为其参数。
  * 这正式称为 currying（柯里化）。
  */
object Currying extends App {

  // 柯里化方法
  def curriedSum(a: Int, b: Int)(c: Int): Int = {
    a + b * c
  }

  // 柯里化函数
  val sumCurring = curriedSum _
  println(sumCurring(10, 20)(30))

  // 当只有一个传入参数时，可以用{}替代()
  val sumInts = curriedSum(10, 20) {
    20
  }
  println(sumInts)

}
