package com.github.tiger.scala.lang.basic

object CaseClasses extends App {

  /**
    * Defining a cass class
    */
  case class Message(sender: String, recipient: String, body: String)

  val message1 = Message("guillaume@quebec.ca", "jorge@catalonia.es", "Ça va ?")

  println(message1.sender) // prints guillaume@quebec.ca
  // message1.sender = "travis@washington.us" // this line does not compile

  /**
    * Comparison
    *
    * Case classes are compared by structure and not by reference
    */
  val message2 = Message("jorge@catalonia.es", "guillaume@quebec.ca", "Com va?")
  val message3 = Message("jorge@catalonia.es", "guillaume@quebec.ca", "Com va?")
  val messagesAreTheSame = message2 == message3 // true

  /**
    * Copying
    *
    * You can create a (shallow) copy of an instance of a case class simply by using the copy method.
    * You can optionally change the constructor arguments.
    */
  val message4 = Message("julien@bretagne.fr",
    "travis@washington.us", "Me zo o komz gant ma amezeg")
  val message5 = message4.copy(sender = message4.recipient, recipient = "claire@bourgogne.fr")
  message5.sender // travis@washington.us
  message5.recipient // claire@bourgogne.fr
  message5.body // "Me zo o komz gant ma amezeg"
}
