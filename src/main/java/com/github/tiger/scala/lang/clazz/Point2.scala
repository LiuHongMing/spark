package com.github.tiger.scala.lang.clazz

/**
  * 私有成员和Getter/Setter语法
  *
  * 注意下对于setter方法的特殊语法
  */
class Point2 {

  private var _x: Int = 0
  private var _y: Int = 0
  private val bound: Int = 100

  def x = _x

  def x_=(newValue: Int): Unit = {
    if (newValue < bound) _x = newValue else printWarning
  }

  // getter
  def y = _y

  // setter（在getter方法的后面加上_=，后面跟着参数。）
  def y_=(newValue: Int): Unit = {
    if (newValue < bound) _y = newValue else printWarning
  }

  private def printWarning = println("WARNING: Out of bounds")
}

object Point2 extends App {
  val point = new Point2()

  point.x = 99
  point.y = 101

}