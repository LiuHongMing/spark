package com.github.tiger.scala.lang.basic

/**
  * 高阶函数将其他函数作为参数或作为结果返回函数。
  * 这是可能的，因为函数是Scala中的第一类值。
  * 这个术语在这一点上可能会有点混乱，
  * 我们对于将函数作为参数或返回函数的方法和函数称之为“高阶函数”。
  */
object HigherOrders extends App {

  val salaries = Seq(20000, 70000, 40000)
  val newSalaries = salaries.map(x => x * 2) // List(40000, 140000, 80000)
  val newSalariesConcise = salaries.map(_ * 2) // List(40000, 140000, 80000)

  def urlBuilder(ssl: Boolean, domainName: String): (String, String) => String = {
    val schema = if (ssl) "https://" else "http://"
    (endpoint: String, query: String) => s"$schema$domainName/$endpoint?$query"
  }

  val domainName = "www.example.com"

  def getURL = urlBuilder(ssl = true, domainName)

  val endpoint = "users"
  val query = "id=1"
  val url = getURL(endpoint, query) // "https://www.example.com/users?id=1": String
}
