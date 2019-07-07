package com.github.tiger.scala.lang.currying.aop

/**
  * 柯里化 -> AOP
  */
object ScopeMock extends App {

  Array("China", "Beijing", "HelloWorld").foreach(length)

  def length(s: String): Int = aopScope {
    s.length
  }

  // 抽象控制
  def aopScope[U](i: => U): U = {
    print(i + " ")
    i
  }

}
