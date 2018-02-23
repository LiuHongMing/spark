package com.github.tiger.scala.basic

object BaseEx {

  def main(args: Array[String]): Unit = {
    println(Time.currentCount())

    val plusOne = new AddOne()
    println(plusOne(1))
  }

  /**
    * 匹配模式
    */
  val time = 1

  time match {
    case i if i == 1 => "one"
    case i if i == 2 => "two"
    case _ => "some other number"
  }

  /**
    * 匹配类型
    *
    * @param o
    * @return
    */
  def bigger(o: Any): Any = {
    o match {
      case i: Int if i < 0 => i - 1
      case i: Int => i + 1
      case d: Double if d < 0.0 => d - 0.1
      case d: Double => d + 0.1
      case text: String => text + "s"
    }
  }

  def calculateType(calc: Calculator): String = {
    calc match {
      case _ if calc.brand == "HP" => "financial"
    }
  }

}

class Foo {}

/**
  * apply方法
  */
object FooMarker {
  def apply: Foo = new Foo()
}

/**
  * 单例对象
  */
object Time {
  var count: Int = 0

  def currentCount(): Long = {
    count += 1
    count
  }
}

class Bar(foo: String)

object Bar {
  def apply(foo: String): Bar = new Bar(foo)
}

/**
  * 函数即对象
  */
class AddOne extends (Int => Int) {
  def apply(m: Int) = m + 1
}

