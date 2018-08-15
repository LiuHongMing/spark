package com.github.tiger.scala.lang.aop

object AopMock extends App {

  Array("China", "Beijing", "HelloWorld").foreach(length)

  def length(s: String): Int = aopScope {
    s.length
  }

  def aopScope[U](i: => U): U = {
    print(i + " ")
    i
  }

}
