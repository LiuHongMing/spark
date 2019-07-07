package com.github.tiger.scala.lang.generics

/**
  * 泛型
  */
object Generics extends App {

  // 泛型

  /**
    * 泛型类
    */
  class Animal[T, S](var name: T, var age: S) {
    println(s"Name is $name, Age is $age")
  }

  val cat = new Animal[String, Int]("小白", 2)
  val dog = new Animal[String, String]("大黄", "5")

  /**
    * 泛型函数
    */
  def asList[T](pSrc: Array[T]): List[T] = {
    if (pSrc.isEmpty) {
      List[T]()
    } else {
      pSrc.toList
    }
  }

  val friends = Array("小白", "琪琪", "乐乐")
  val friendList = asList[String](friends)
  println("friendList.isInstanceOf[List[String]]",
    friendList.isInstanceOf[List[String]])

  // 类型界定

  /**
    * 上边界
    *
    * 表达了泛型的类型必须是"某种类型"或某种类型的"子类"
    */
  class Pair[T <: Comparable[T]](first: T, second: T) {
    def smaller = {
      first.compareTo(second) match {
        case x if x < 0 => println("first < second")
        case x if x > 0 => println("first > second")
        case _ => println("first = second")
      }
    }
  }

  val p1 = new Pair[String]("10", "20")
  p1.smaller
  val p2 = new Pair[String]("20", "20")
  p2.smaller

  /**
    * 下边界
    *
    * 表达了泛型的类型必须是"某种类型"或某种类型的"父类"
    */
  class Father(val name: String)

  class Child(name: String) extends Father(name)

  def getIDCard[R >: Child](person: R) = {
    person match {
      case person if person.isInstanceOf[Child] => println("please tell us you parent's name.")
      case person if person.isInstanceOf[Father] => println("sign you name for your child's id card ")
      case _ => println("sorry, you are not allowed to get id card")
    }
  }

  val c = new Child("Alice")
  getIDCard(c)
  val f = new Father("Tom")
  getIDCard(f)


  class Person(val name: String) {
    def talk(person: Person) = {
      val pName = person.name
      println(s"$name speak to $pName")
    }
  }

  class Worker(name: String) extends Person(name)

  class Dog(val name: String)

  /**
    * 视图边界
    *
    * 用一个可用的隐式转换函数（隐式视图）来将一种类型自动转换为另外一种类型
    */
  class Cube[T <% Person](p1: T, p2: T) {
    def conmmunicate = p1.talk(p2)
  }

  implicit def dog2Person(dog: Dog) = new Person(dog.name)

  val p = new Person("Spark")
  val d = new Dog("大黄")
  val cube = new Cube[Person](p, d)
  cube.conmmunicate

  // 协变（Covariant）指定类型为某类时，传入其子类或其本身
  // 逆变（Contravariant）指定类型为某类时，传入其父类或其本身

  class Engineer

  class Expert extends Engineer

  /**
    * 协变 [+T]
    *
    * 可以传入 T 或 T 的子类
    */
  class Meeting[+T]

  def participateMeeting(meeting: Meeting[Engineer]) = println("welcome (Covariant)")

  participateMeeting(new Meeting[Engineer])
  participateMeeting(new Meeting[Expert])

  class Master(val name: String)

  class Professor(name: String) extends Master(name)

  class Teacher(name: String) extends Professor(name)

  /**
    * 逆变 [-T]
    *
    * 可以传入 T 或 T 的父类
    */
  class Meeting2[-T]

  def participateMeeting2(meeting: Meeting2[Professor]) = println("welcome (Contravariant)")

  participateMeeting2(new Meeting2[Master])
  participateMeeting2(new Meeting2[Professor])

  // 错误，Worker 不是 Professor 的父类
  // participateMeeting2(new Meeting2[Worker])

  // T: Ordering
  class Maximum[T: Ordering](x: T, y: T) {
    def bigger(implicit ord: Ordering[T]) = {
      if (ord.compare(x, y) > 0) x else y
    }
  }

  println(new Maximum[Int](3, 5).bigger)

}
