package com.github.tiger.scala.lang.basic

/**
  * 通过混入（MIXIN）来组合类
  *
  * Mixins are traits which are used to compose a class.
  */
object Mixins extends App {

  abstract class A {
    val message: String
  }

  class B extends A {
    val message = "I'm an instance of class B"
  }

  trait C extends A {
    def loudMessage = message.toUpperCase()
  }

  class D extends B with C

  val d = new D
  println(d.message) // I'm an instance of class B
  println(d.loudMessage) // I'M AN INSTANCE OF CLASS B

  abstract class AbsIterator {
    type T

    def hasNext: Boolean

    def next(): T
  }

  class StringIterator(s: String) extends AbsIterator {
    type T = Char

    private var i: Int = 0

    override def hasNext: Boolean = i < s.length

    override def next(): Char = {
      val ch = s charAt i
      i += 1
      ch
    }
  }

  trait RichIterator extends AbsIterator {
    def foreach(f: T => Unit): Unit = while(hasNext) f(next() )
  }

  class RichStringIterator extends StringIterator("Scala") with RichIterator

  val richStringIterator = new RichStringIterator

  richStringIterator foreach println

}


