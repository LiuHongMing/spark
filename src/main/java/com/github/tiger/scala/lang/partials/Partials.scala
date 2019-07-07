package com.github.tiger.scala.lang.partials

/**
  * 偏函数
  */
object PartialFunctions extends App {

  /**
    * 定义
    */
  val one: PartialFunction[Int, String] = {
    case 1 => "one"
  }

  /**
    * 调用
    */
  println(one(1))

  val two: PartialFunction[Int, String] = {
    case 2 => "two"
  }

  val three: PartialFunction[Int, String] = {
    case 3 => "three"
  }

  val wildcard: PartialFunction[Int, String] = {
    case _ => "something else"
  }

  /**
    * 组合
    */
  val partial = one orElse two orElse three orElse wildcard

  println(partial(5))
  println(partial(4))
  println(partial(3))
  println(partial(2))
  println(partial(1))

}
