/**
  * Created by Tam.Laniado on 24/10/2016.
  */

/* Codelauncher.scala */

package scalaexamples



object Codelauncher {
  def main(args: Array[String]) {

    val logFile = "C:\\Users\\Tam.Laniado\\spark-2.0.1-bin-hadoop2.7\\README.md" // Should be some file on your system

//        var people = new Array[String](3)
//
//        people(0) = "Zara"; people(1) = "Nuha"; people(4/2) = "Ayan"

        val people = Array("Zara", "Nuha", "Ayan")


//    val helloWorldObj = HelloWorldObj;helloWorldObj.sayHello(people)
//    val helloWorldClass = new HelloWorldClass();helloWorldClass.sayHello(people)
//    val sparkExample1 = SparkExample1;sparkExample1.doAnalysis()
    val sparkExample2 = SparkExample2;sparkExample2.doAnalysis("and")
//    val sparkExample3 = SparkExample3;sparkExample3.doAnalysis()
//    val sparkSessionExample1 = SparkSessionExample1;sparkSessionExample1.doAnalysis()
//    val sparkSessionExample2 = SparkSessionExample2;sparkSessionExample2.doAnalysis()
//    val sparkSessionExample3 = SparkSessionExample3;sparkSessionExample3.doAnalysis()
//    val sparkSessionExample4 = SparkSessionExample4;sparkSessionExample4.doAnalysis()
//    val sparkSessionExample5 = SparkSessionExample5;sparkSessionExample5.doAnalysis()
//    val sparkSessionExample6 = SparkSessionExample6;sparkSessionExample6.doAnalysis()
  }
}
