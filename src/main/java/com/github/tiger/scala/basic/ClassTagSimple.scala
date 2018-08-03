package com.github.tiger.scala.basic

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

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

  // implicit m: ClassTag[T] 改成implicit m: TypeTag[T]也是可以的
  def manif3[T](x: List[T])(implicit m: TypeTag[T]) = {
    println(x)
  }

  def main(args: Array[String]): Unit = {
    manif3(List("Scala", 3))
  }
}
