package com.github.tiger.scala.lang.traits

import scala.collection.mutable.ArrayBuffer

object Traits extends App {

  val iterator = new IntIterator(10)
  println(iterator.next())
  println(iterator.next())
  println(iterator.next())

  val bmw = new BMW("BMW", 12)
  println(bmw.brand, bmw.shineRefraction)

  val cat = new Cat("Harry")
  val dog = new Dog("Sally")

  val animals = ArrayBuffer.empty[Pet]
  animals.append(cat)
  animals.append(dog)
  animals.foreach(pet => println(pet.name))

}

trait Iterator[A] {
  def hasNext: Boolean

  def next(): A
}

class IntIterator(to: Int) extends Iterator[Int] {
  private var current = 0

  override def hasNext: Boolean = current < to

  override def next(): Int = {
    if (hasNext) {
      val t = current
      current += 1
      t
    } else 0
  }
}

/**
  * 子类型
  *
  * 凡是需要特质的地方，都可以由该特质的子类型来替换。
  */
trait Pet {
  val name: String // 在这里 trait Pet 有一个抽象字段 name
}

// name 由 Cat 和 Dog 的构造函数中实现
class Cat(val name: String) extends Pet

class Dog(val name: String) extends Pet

// --------------------

/**
  * 定义一个特质
  */
trait Car {
  val brand: String
}

trait Shiny {
  val shineRefraction: Int
}

/**
  * 通过with关键字，一个类可以扩展多个特质
  */
class BMW(val brand: String, val shineRefraction: Int)
  extends Car with Shiny
