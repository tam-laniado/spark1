package scalaexamples

class HelloWorldClass {
  def sayHello(args: Array[String]): Unit = {
    println("Inside HelloWorldClass")
    println("Hello, world!")
    println("And in particular, hello to :")
    for(arg<-args)
      println(arg)+","
  }
}
