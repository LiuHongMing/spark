package com.github.tiger.scala.lang

import scala.annotation.tailrec

object TailRecursion {

  @tailrec
  def sum(n: Long, total: Long): Long = {
    if (n <= 0) total
    else sum(n - 1, total + n)
  }

  def main(args: Array[String]) {
    val total = sum(10000000, 0)
    println(total)
  }

}
