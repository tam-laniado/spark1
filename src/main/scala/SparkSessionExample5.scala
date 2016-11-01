package scalaexamples
/* SparkSessionExample5.scala */

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

//the following has to be defined OUTSIDE the method
case class Person(ID: Int,
                  Name: String,
                  Sex: String,
                  Age: Int)

object SparkSessionExample5 {//loading a csv into a dataset
  def doAnalysis() {
    println("SSparkSessionExample5")

    val sourceFile = "C:\\Users\\Tam.Laniado\\data\\tamtest.csv"

    val spark = SparkSession.builder().
      master("local[*]")
      .appName("Spark dataset test")
      .config("spark.sql.warehouse.dir", "file:///C:/Users/Tam.Laniado/Projects/scala/scratch/spark-warehouse/")//needed for uppity windows config, note double quotes
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()//reduce the verbosity of spark output, can also be done via editing spark/conf/log4j.properties
    rootLogger.setLevel(Level.WARN)

    println("spark session initialised")


    import spark.implicits._


  //very simple dataset load, note a) above import implicits (needs doing after init of spark session) and b) case class needs init outside this method

    val ds1 = Seq(Person(1,"Tam","M",34),Person(2,"Tamm","M",37)).toDS()

    println("dataset populated from a hard coded sequence")

    ds1.take(4).foreach(println)


    //dataset load from csv file
    println("dataset populated from a csv")
    val peopleDs = spark.read
      .option("delimiter",",").
      option("header","true")
      .option("inferSchema", "true")//logically this doesnt seem to be needed.  But it is required
        .csv(sourceFile).as[Person]

    peopleDs.take(4).foreach(println)

    spark.stop()
  }
}