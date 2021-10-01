package lec1

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object MainPipeline extends App {

//  val spark = SparkSession.builder()
//    .appName("DataFrames Basics")
//    .config("spark.master", "local")
//    .getOrCreate()
//
//
//
//  val firstDF: DataFrame = spark.read
//    .format("json")
//    .option("inferSchema", "true")
//    .load("src/main/resources/data/cars.json")
//
//  // CTRL + P to show args
//  firstDF.show()
//  firstDF.printSchema()
//
//
//  firstDF.take(10).foreach(println)
//  val rowExmpl: Row = firstDF.head()
//  val firstDfAnalog = spark.read.json("src/main/resources/data/cars.json")

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

//  val carsSchema = StructType(Array(
//    StructField("Name", StringType),
//    StructField("Miles_per_Gallon", DoubleType),
//    StructField("Cylinders", LongType),
//    StructField("Displacement", DoubleType),
//    StructField("Horsepower", LongType),
//    StructField("Weight_in_lbs", LongType),
//    StructField("Acceleration", DoubleType),
//    StructField("Year", DateType),
//    StructField("Origin", StringType)
//  ))
//
//  val carsDF = spark.read
//    .format("json")
//    .schema(carsSchema) // enforce a schema
//    .option("mode", "failFast") // dropMalformed, permissive (default)
//    .option("path", "src/main/resources/data/cars.json")
//    .load()
//
//  // carsDF.write.format("json").save("src/main/resources/data/cars_duplicate.json")
//  // val dup = spark.read.json("src/main/resources/data/cars_duplicate.json")
//  // dup.repartition(100).write.json("src/main/resources/data/cars_triplicate.json")
//
//  val csv = spark.read
//    .option("sep", ",")
//    .option("header", "true")
//    .csv("src/main/resources/data/stocks.csv")
//
//  // csv.write.parquet("src/main/resources/data/parquet_test")
//  print(csv.count)

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  val nameCol: Column = carsDF.col("Name")
  carsDF.select(nameCol).printSchema()
  carsDF.select("Name").printSchema()
  carsDF.select(col("Name")).printSchema

  import spark.implicits._
  carsDF.select('Name).printSchema()
  carsDF.select($"Name").show()


  carsDF.select($"Name", $"Year").show()
  val sq = Seq("Name", "Year").map(col)
  carsDF.select(sq:_*)

  val divideByTwo = col("Weight_in_lbs") / 2
  carsDF.select(divideByTwo).show()

  carsDF.withColumn("divByTwo", divideByTwo)

  carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")

  carsDF.drop("Weight_in_lbs", "Name")

  carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") =!= 130).show()

  carsDF.filter((col("Origin") === "USA") and (col("Horsepower") =!= 130)).show()

  carsDF.select("Origin").distinct().show
}
