package com.github.tiger.scala.lang.clazz

/**
  * 定义类，构造器
  */
class Point(var x: Int = 0, var y: Int = 0) {

  def move(dx: Int, dy: Int): Unit = {
    x += dx
    y += dy
  }

  override def toString: String = {
    s"($x, $y)"
  }
}

object Point extends App {

  val origin = new Point
  val point1 = new Point(x = 1)
  val point2 = new Point(y = 2)

  println(origin, point1, point2)
}

