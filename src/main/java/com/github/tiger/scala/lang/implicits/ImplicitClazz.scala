package com.github.tiger.scala.lang.implicits

/**
  * Scala 2.10引入了一种叫做隐式类的新特性。
  *
  * 隐式类指的是用implicit关键字修饰的类。
  *
  * 在对应的作用域内，带有这个关键字的类的主构造函数可用于隐式转换。
  */
object ImplicitClazz {

  /**
    * 隐式类
    */
  implicit class Calculator(x: Int) {
    def add_10(a: Int): Int = (x + a) * 10
  }

  implicit class Helpers(s: String) {
    implicit def random(): String = {
      val r = "%.2f".format(Math.random());
      val res = s"$s,$r"
      res
    }
  }

}
