package com.github.tiger.scala.lang.implicits

/**
  * Scala隐式转换类型主要包括以下几种类型：
  *
  * 隐式参数、隐式视图、隐式类。
  *
  */
object ImplicitBasic extends App {

  /**
    * 导入隐式类
    */
  import ImplicitClazz._

  println(8.add_10(10))

  println("hello".random)

}
