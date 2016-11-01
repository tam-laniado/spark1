package scalaexamples

object HelloWorldObj {
  def sayHello(args: Array[String]){
    println("Inside HelloWorldObj")
    println("Hello, world!")
    println("And in particular, hello to :")
    for(arg<-args)
      println(arg)+","

  }
}
