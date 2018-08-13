package com.github.tiger.scala.lesson.collections

object Collections {

  def main(args: Array[String]): Unit = {
    /**
      * Traversable
      */
    val xt = Traversable(1, 2, 3)
    val yt = Traversable(4, 5, 6)

    // 加运算
    val traversable = xt ++ yt // List(1, 2, 3, 4, 5, 6)
    println(traversable.toString())

    // 子容器
    val filter = traversable.withFilter(a => {
      a % 2 == 0
    })
    filter.foreach(a => printf("%d ", a)) // List(2, 4, 6)
    println(filter)
    val withFilter = traversable.withFilter(a => {
      a % 2 == 0
    })
    withFilter.foreach(a => printf("%d ", a)) // List(2, 4, 6)
    println(withFilter)

    // 拆分
    val partitions = traversable.partition(p => p % 2 == 0)
    println(partitions._1) // List(2, 4, 6)
    println(partitions._2) // List(1, 3, 5)

    /**
      * Iterable
      */
    val xi = Iterable("a", "b", "c")
    val yi = Iterable("x", "y", "z")
    val iterable = xi ++ yi
    println(iterable.toString())

    /**
      * List
      */
    val xl = List(1, 2, 3)
    val yl = List(4, 5, 6)

    // 叠加
    val list1 = xl :: yl // List(List(1, 2, 3), 4, 5, 6)
    val list2 = xl ::: yl // List(1, 2, 3, 4, 5, 6)
    println(list1.toString())
    println(list2.toString())

    // 特殊折叠
    println(xl.sum) // 返回容器xs中数字元素的和
    println(xl.product) // 返回容器中数字元素的积
  }

}
