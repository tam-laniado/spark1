package scalaexamples
/* SparkExample2.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkExample2 {//source data is loaded from small text file
  def doAnalysis(textMatch : String) : Long={

    println("SparkExample2")

    val logFile = "C:\\Users\\Tam.Laniado\\spark-2.0.1-bin-hadoop2.7\\README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
      .setMaster("local")//needed for non shell dev


    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")//remove info reporting

    val logData = sc.textFile(logFile, 2).cache()

    val logDataMatch = logData.filter(line=>line.contains(textMatch))

    val countMatchingLines  = logDataMatch.count()

    println("Number of matching lines : "+countMatchingLines)

    println("\nOutput entire RDD, a very bad idea\n")
    logDataMatch.collect().foreach(println)
    println("\nOutput first few lines of RDD, a much better idea\n")
    logDataMatch.take(4).foreach(println)


    sc.stop()

    return countMatchingLines
  }
}
