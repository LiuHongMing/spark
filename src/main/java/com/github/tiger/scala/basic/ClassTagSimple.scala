package com.github.tiger.scala.basic

import scala.reflect.ClassTag

class MyType[T]

object TestClassTag {

  def arrayMake[T: ClassTag](first: T, second: T) = {
    val r = new Array[T](2)
    r(0) = first
    r(1) = second
    r
  }

  def arrayMake2[T](first: T, second: T)(implicit m: ClassTag[T]) = {
    val r = new Array[T](2)
    r(0) = first
    r(1) = second
    r
  }

  def manif[T](x: List[T])(implicit m: Manifest[T]) = {
    if (m <:< manifest[String])
      println("List strings")
    else
      println("Some other type")
  }

}
