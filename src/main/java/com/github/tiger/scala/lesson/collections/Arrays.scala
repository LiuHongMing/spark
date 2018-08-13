package com.github.tiger.scala.lesson.collections

object Arrays {

  def main(args: Array[String]): Unit = {

    val a1 = Array(1, 2, 3)

    val a2 = a1 map (_ * 3)

    val a3 = a2.filter(_ % 2 == 0)

    val a4 = a3.reverse
  }

}
