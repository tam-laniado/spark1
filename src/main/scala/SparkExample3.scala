package scalaexamples
/* SparkExample3.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf

object SparkExample3 {//source data is loaded from small text file, applies a udf to output
  def doAnalysis() {

    println("SparkExample3")

    val logFile = "C:\\Users\\Tam.Laniado\\spark-2.0.1-bin-hadoop2.7\\README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
      .setMaster("local")//needed for non shell dev


    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")//remove info reporting

    val upper: String => String = _.toUpperCase
    val upperUDF = udf(upper)


    val logData = sc.textFile(logFile, 2).cache()

    val logDataError = logData.filter(line=>line.contains("and"))
    println("\nOutput entire RDD, a very bad idea\n")
    logDataError.collect().foreach(x => { println(x)})
    println("\nOutput first few lines of RDD, a much better idea\n")
    logDataError.take(4).foreach(x => { println(upper(x))})


    sc.stop()
  }
}
