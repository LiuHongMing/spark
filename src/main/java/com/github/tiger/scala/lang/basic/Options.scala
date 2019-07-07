package com.github.tiger.scala.lang.basic

/**
  * Option本身是泛型的，并且有两个子类： Some[T] 或 None
  */
object Options extends App {


  def show(s: Option[String]) = s match {
    case Some(s) => s
    case None => "?"
  }

  val value = Option("Wo Wo")
  println(show(value))

  val nullValue = Option(null)
  println(show(nullValue))

}
