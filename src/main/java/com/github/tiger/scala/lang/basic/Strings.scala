package com.github.tiger.scala.lang.basic

/**
  * Scala提供了一种新的机制来根据数据生成字符串：字符串插值。
  *
  * 字符串插值允许使用者将变量引用直接插入处理过的字面字符中。
  */
object Strings extends App {

  /**
    * s 字符串插值器
    *
    * 字符串插值器也可以处理任意的表达式
    */
  val name = "James"
  println(s"Hello,$name") // Hello,James

  println(s"1+1=${1 + 1}")

  /**
    * f 插值器
    *
    * James is 1.90 meters tall f 插值器是类型安全的。
    *
    * 如果试图向只支持 int 的格式化串传入一个 double 值，编译器则会报错
    */
  val height = 1.9d
  println(f"$name%s is $height%2.2f meters tall")

  /**
    * raw 插值器
    */

  val r = raw"a\nb"
  println(r)

}
