package com.github.tiger.scala.lang.basic

/**
  * 方法表达式说明
  */
object Methods extends App {

  /**
    * Methods are defined with the def keyword.
    * def is followed by a name, parameter lists, a return type, and a body.
    */
  def add(x: Int, y: Int): Int = x + y

  /**
    * Methods can take multiple parameter lists.
    */
  def addThenMultiply(x: Int, y: Int)(multiplier: Int): Int = (x + y) * multiplier

  /**
    * Or no parameter lists at all.
    */
  def name: String = System.getProperty("user.name")

  /**
    * Methods can have multi-line expressions as well.
    *
    * The last expression in the body is the method’s return value.
    */
  def getSquareString(input: Double): String = {
    val square = input * input
    square.toString
  }

  /**
    * 以无参函数作为参数传入
    */
  def withScope[U](body: => U): Any = Seq.empty[Int]

}
