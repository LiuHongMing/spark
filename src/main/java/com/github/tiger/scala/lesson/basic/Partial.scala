package com.github.tiger.scala.lesson.basic

/**
  * 部分应用（Partial application）
  *
  * 在调用时，partial在传入部分参数时，必须显示指定剩余参数的占位符
  *
  * 类型是普通函数类型
  */
object Partial {

  def addMethod(m: Int, n: Int) = m + n

  /**
    * 在方法名后加入空格下划线，将方法转换成函数
    */
  val addFunc = addMethod(2, _: Int)

  def main(args: Array[String]): Unit = {
    println(addFunc(3))
  }

}
