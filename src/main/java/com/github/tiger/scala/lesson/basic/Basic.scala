package com.github.tiger.scala.lesson.basic

object Basic {

  /**
    * 可变长参数
    *
    * @param arg
    */
  def capitalizeAll(arg: String*): Unit = {
    arg.map {
      arg => arg.capitalize
    }
  }

  def main(args: Array[String]): Unit = {
    val cal = new Calculator("HP", 88)
    println(cal.brand, cal.color, cal.price, cal.level)

    val c = new Circle(2)
    println(c.getArea())
  }

}

/**
  * 类
  *
  * 构造函数：构造函数不是特殊的方法，他们是除了类的方法定义之外的代码。
  *
  * @param brand
  */
class Calculator(val brand: String, val price: Int) {

  /**
    * constructor
    */
  val color: String = if (brand == "TI") {
    "blue"
  } else if (brand == "HP") {
    "black"
  } else {
    "white"
  }

  val level: String = if (price >= 80) {
    "high"
  } else if (price >= 60 && price < 80) {
    "medium"
  } else {
    "low"
  }

  // An instance method
  def addOne(x: Int, y: Int): Int = x + y
}

/**
  * 继承
  *
  * @param brand
  * @param price
  */
class ScientificCalculator(brand: String, price: Int)
  extends Calculator(brand, price) {
  def log(m: Double, base: Double) = math.log(m) / math.log(base)
}

/**
  * 重载方法
  *
  * @param brand
  * @param price
  */
class EvenMoreScientificCalculator(brand: String, price: Int)
  extends ScientificCalculator(brand, price) {
  def log(m: Int): Double = log(m, math.exp(m))
}

/**
  * 抽象类
  */
abstract class Shape() {
  def getArea(): Int
}

class Circle(r: Int) extends Shape {
  override def getArea(): Int = (r * r * 3)
}

/**
  * 特质
  */
trait Car {
  val brand: String
}

trait Shiny {
  val shineRefraction: Int
}

trait BMW extends Car with Shiny {
  val brand = "BMW"
  val shineRefraction = 12
}

/**
  * 保持traits简短并且是正交的：不要把分离的功能混在一个trait里，考虑将最小的相关的意图放在一起。
  */
trait Reader {
  def read(n: Int): Array[Byte]
}

trait Writer {
  def write(bytes: Array[Byte])
}

/**
  * 类型
  *
  * @tparam K
  * @tparam V
  */
trait Cache[K, V] {
  def get(key: K): V

  def put(key: K, value: V)

  def delete(key: K)
}

trait Driver {
  def remove[K](key: K)
}
