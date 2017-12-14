package scala

trait Queue[T] {
  def head: T

  def tail: Queue[T]

  def append(x: T): Queue[T]
}

object Queue {

//  def apply[T](xs: T*) = new QueueImpl[T](xs.toList, Nil)
//
//  private class QueueImpl[T](private val leading: List[T], private val trailing: List[T])
//    extends Queue[T] {
//
//    def mirror = if (leading.isEmpty) new QueueImpl(trailing.reverse, Nil) else this
//
//    def head: T = mirror.leading.head
//
//    def tail: QueueImpl[T] = {
//      val q = mirror
//      new QueueImpl[T](q.leading.tail, q.trailing)
//    }
//
//    def append(x: T): Queue[T] = new QueueImpl(leading, x :: trailing)
//
//  }
//
//  def main(args: Array[String]) {
//    val queue = Queue(1, 2, 3)
//    print(queue)
//  }

}
