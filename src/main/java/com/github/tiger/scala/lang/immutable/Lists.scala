package com.github.tiger.scala.lang.immutable

object Lists extends App {

  val first = List[Int](1, 2, 3)

  val second = 4 :: first :: Nil

  val third = 5 :: second

  println(first)
  println(second)
  println(third)

  val fourth = List(6) +: first
  val fifth = List(7) :+ first

  println(fourth)
  println(fifth)

}
