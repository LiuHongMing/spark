package com.github.tiger.scala.lang.reflect

import scala.reflect._
import scala.reflect.runtime.universe._

/**
  * TypeTags and Manifests
  */
object Manifests extends App {

  /**
    * scala.reflect.api.TypeTags#TypeTag
    *
    * Scala类型的完整类型描述符
    */
  val tt = typeTag[Int]

  println(tt)

  /**
    * 使用类型参数的上下文绑定
    */
  def paramInfo[T: TypeTag](x: T): Unit = {
    val targs = typeOf[T] match {
      case TypeRef(_, _, args) => args
    }
    println(s"type of $x has type arguments $targs")
  }

  paramInfo(42)

  paramInfo(List(1, 2))

  /**
    * scala.reflect.ClassTag
    *
    * Scala类型的部分类型描述符
    */
  val ct = classTag[String]

  println(ct)

  def mkArray[T: ClassTag](elems: T*) = Array[T](elems: _*)

  mkArray(1, 2, 3, 4, 5).foreach(println)

  /**
    * scala.reflect.api.TypeTags#WeakTypeTag
    *
    * 抽象类型的类型描述符
    */

  def weakParamInfo[T](x: T)(implicit tag: WeakTypeTag[T]): Unit = {
    val targs = tag.tpe match {
      case TypeRef(_, _, args) => args
    }
    println(s"type of $x has type arguments $targs")
  }

  def foo[T] = weakParamInfo(List[T]())

  foo[Int]

}
