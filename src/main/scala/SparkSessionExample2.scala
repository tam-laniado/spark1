/**
  * Created by Tam.Laniado on 25/10/2016.
  */
package scalaexamples

import org.apache.spark.sql.SparkSession


object SparkSessionExample2 {
  def doAnalysis(): Unit ={

    println("SparkSessionExample2")

    val sourceFile = "C:\\Users\\Tam.Laniado\\data\\dcm_account1137_impression_2016042518_20160426_033128_238398981.csv.gz"

    val spark = SparkSession.builder().
      master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "file:///C:/Users/Tam.Laniado/Projects/scala/scratch/spark-warehouse/")//needed for uppity windows config, note double quotes
      .getOrCreate()

    println("spark session initialised")



    val myDf = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(sourceFile)


    myDf.createOrReplaceTempView("myTable")

    val SQLresult=spark.sql("select count(*) FROM myTable")



    println(SQLresult.show(10))//although it'll only generate a more one record
    spark.stop()


  }

}
