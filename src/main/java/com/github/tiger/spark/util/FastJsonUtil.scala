package com.github.tiger.spark.util

import com.alibaba.fastjson.JSON
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

object FastJsonUtil {

  val logger = LoggerFactory.getLogger(FastJsonUtil.getClass)

  def convertJson2Type[T](json: String)(implicit ct: ClassTag[T]): T = {
    Option(json) match {
      case Some(s) if !s.isEmpty =>
        return JSON.parseObject(json, ct.runtimeClass).asInstanceOf[T]
    }
  }

  def tryConvert[T: ClassTag](json: String): (Boolean, Any) = {
    try {
      val result = convertJson2Type[T](json)
      return (true, result)
    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage)
      }
    }
    (false, None)
  }

}
