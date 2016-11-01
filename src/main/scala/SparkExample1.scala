package scalaexamples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkExample1 {//source data defined in code
  def doAnalysis() {
    println("SparkExample1")

    val conf = new SparkConf().setAppName("Simple Application")
      .setMaster("local")//needed for non shell dev


    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")//remove info reporting

    val linesVeryLocal  = sc.parallelize(List("Because the plot thickens every day", "As the pieces of my puzzle keep a crumbling away"))


    val numThes = linesVeryLocal.filter(line => line.contains("the")).count()
    val numPlots = linesVeryLocal.filter(line => line.contains("plot")).count()
    val numPuzzles = linesVeryLocal.filter(line => line.contains("puzzle")).count()
    println("Lines with the: %s, Lines with plot: %s, Lines with puzzle: %s".format(numThes, numPlots,numPuzzles))

    sc.stop()

  }
}
