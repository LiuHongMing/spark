package com.github.tiger.scala.lang.implicits

abstract class Monoid[A] {
  def add(x: A, y: A): A

  def unit: A
}

/**
  * 隐式参数
  */
object ParameterDemo extends App {

  implicit val stringMonoid: Monoid[String] = new Monoid[String] {
    override def add(x: String, y: String): String = x concat y

    override def unit: String = ""
  }

  implicit val intMonoid: Monoid[Int] = new Monoid[Int] {
    override def add(x: Int, y: Int): Int = x + y

    override def unit: Int = 0
  }

  def sum[A](xs: List[A])(implicit m: Monoid[A]): A = {
    if (xs.isEmpty) m.unit
    else m.add(xs.head, sum(xs.tail))
  }

}
