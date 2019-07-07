package com.github.tiger.spark.util

import java.math.BigInteger
import java.security.MessageDigest

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object Md5Util {

  val _algorithm = "MD5"

  def md5(str: String) = try {
    val md = MessageDigest.getInstance(_algorithm)
    md.update(str.getBytes)
    new BigInteger(1, md.digest).toString(16)
  } catch {
    case e: Exception =>
      throw new Exception("MD5加密错误", e)
  }

  def main(args: Array[String]): Unit = {
    val s = ArrayBuffer((1, 2))

    s += ((2, 3))

    println(s)

    val condition = "JOB_NATURE:2 OR JOB_NATURE:4 OR JOB_NATURE:5"
    val pattern = new Regex(s"(CITY_ID|JOB_NATURE|JOB_TYPE|INDUSTRY_ID|COMPANY_TYPE):([0-9]+)")
    (pattern findAllIn condition).matchData foreach (
      m => println(m.group(0)))

  }
}
