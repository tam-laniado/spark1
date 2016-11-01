package scalaexamples
/* SparkSessionExample6.scala */

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}


object SparkSessionExample6 {//loading a dataframe into a dataset
def doAnalysis() {
  println("SparkSessionExample6")

  val sourceFile = "C:\\Users\\Tam.Laniado\\data\\tamtest.csv"

  val spark = SparkSession.builder().
    master("local[*]")
    .appName("Spark SQL basic example")
    .config("spark.sql.warehouse.dir", "file:///C:/Users/Tam.Laniado/Projects/scala/scratch/spark-warehouse/")//needed for uppity windows config, note double quotes
    .getOrCreate()
  val rootLogger = Logger.getRootLogger()//reduce the verbosity of spark output, can also be done via editing spark/conf/log4j.properties
  rootLogger.setLevel(Level.WARN)


  println("spark session initialised")

  import spark.implicits._

  val myDf = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(sourceFile)


  val myDs = myDf.as[Person]


  myDs.take(4).foreach(println)


  spark.stop()
}
}