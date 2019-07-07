package com.github.tiger.scala.lang.basic

import scala.util.Random

/**
  * 提取器对象是具有unapply方法的对象。
  * 虽然apply方法就像一个接受参数并创建一个对象的构造函数，但是unapply接受一个对象并试图返回参数。
  * 这通常用于模式匹配和部分功能。
  */
object Extractors extends App {

  object CustomerID {
    def apply(name: String): String = s"$name--${Random.nextLong}"

    def unapply(customID: String): Option[String] = {
      val stringArray: Array[String] = customID.split("--")
      if (stringArray.tail.nonEmpty) Some(stringArray.head) else None
    }
  }

  val customer1ID = CustomerID("Sukyoung")
  customer1ID match {
    case CustomerID(name) => println(name)
    case _ => println("Could not extract a CustomerID")
  }

  /**
    * unapply方法也可用于分配值。
    */
  val customer2ID = CustomerID("Nico")
  val CustomerID(name) = customer2ID
  println(name) // prints Nico

  /**
    * This is equivalent to val name = CustomerID.unapply(customer2ID).get
    */
  val CustomerID(name2) = "--asdfasdfasdf"
  println(name2)

  /**
    * If there is no match, a scala.MatchError is thrown
    */
  // val CustomerID(name3) = "-asdfasdfasdf"

}
