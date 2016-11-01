/**
  * Created by Tam.Laniado on 25/10/2016.
  */
package scalaexamples


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}


object SparkSessionExample4 {//import csv into dataframe, save some of the fields as parquet, loads parquet file to confirm it has worked
  def doAnalysis(): Unit ={

    println("SparkSessionExample5")

    val sourceFile="dcm_account1137_impression_2016042518_20160426_033128_238398981"

    val sourcePath = "C:\\Users\\Tam.Laniado\\data\\"

    val sourceFilePath = sourcePath+sourceFile+".csv.gz"

    val destFilePath = sourcePath+sourceFile+".parquet"

    val spark = SparkSession.builder().
      master("local")
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "file:///C:/Users/Tam.Laniado/Projects/scala/scratch/spark-warehouse/")//needed for uppity windows config, note double quotes
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()//reduce the verbosity of spark, can also be done via editing spark/conf/log4j.properties
    rootLogger.setLevel(Level.ERROR)

    println("spark session initialised")


    val myDf = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(sourceFilePath)

    println(myDf.schema)

    myDf.createOrReplaceTempView("csvDataLoad")

    val sqlDf=spark.sql("select `User ID` AS USER_ID,`Advertiser ID` AS ADVERTISER_ID,`Campaign ID` AS CAMPAIGN_ID FROM csvDataLoad")

    sqlDf.write.format("parquet").mode(SaveMode.Overwrite).save(destFilePath)


    println("showing the just saved parquet data")

    val parquetFileDF = spark.read.parquet(destFilePath)

    parquetFileDF.createOrReplaceTempView("parquetDataLoad")

    println(parquetFileDF.schema)

    val SQLresult=spark.sql("select count(*) FROM parquetDataLoad")


    println(SQLresult.show(10))//although it'll only generate a more one record

    spark.stop()
  }

}
