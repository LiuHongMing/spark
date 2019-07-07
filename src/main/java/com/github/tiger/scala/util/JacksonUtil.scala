package com.github.tiger.scala.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JacksonUtil extends ObjectMapper {

  registerModule(DefaultScalaModule)

  def main(args: Array[String]): Unit = {
    val text = "{\"name\":\"Andy\", \"age\":30}"
    val pp = readValue(text, classOf[Map[String, Any]])
    println(pp)
  }

}
