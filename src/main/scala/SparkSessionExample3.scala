/**
  * Created by Tam.Laniado on 25/10/2016.
  */
package scalaexamples

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf

object SparkSessionExample3 {
  def doAnalysis(): Unit ={

    println("SparkUdfExample3")

    val sourceFile = "C:\\Users\\Tam.Laniado\\data\\dcm_account1137_impression_2016042518_20160426_033128_238398981.csv.gz"

    val spark = SparkSession.builder().
      master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "file:///C:/Users/Tam.Laniado/Projects/scala/scratch/spark-warehouse/")//needed for uppity windows config, note double quotes
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()//reduce the verbosity of spark, can also be done via editing spark/conf/log4j.properties
    rootLogger.setLevel(Level.WARN)

    println("spark session initialised")

    val lower: String => String = _.toLowerCase
    val lowerUDF = udf(lower)

    val myDf = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(sourceFile)

    println(myDf.schema)


    myDf.createOrReplaceTempView("myTable")



    val sqlDf=spark.sql("select `User ID` AS USER_ID FROM myTable")//note back ticks needed to handle fields names with spaces in them

    sqlDf.withColumn("lower", lowerUDF(sqlDf.col("USER_ID"))).show(10)


    spark.stop()


  }

}
