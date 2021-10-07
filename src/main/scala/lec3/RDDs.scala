package lec3

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))
  stocksRDD.getNumPartitions

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose the type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // you get to keep type info

  // Transformations

  // distinct
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager ACTION

  // counting
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // z

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive

  // Partitioning
//
//  val repartitionedStocksRDD = stocksRDD.repartition(30)
//  repartitionedStocksRDD.toDF.write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/stocks30")
//  /*
//    Repartitioning is EXPENSIVE. Involves Shuffling.
//    Best practice: partition EARLY, then process that.
//    Size of a partition 10-100MB.
//   */
//
//  // coalesce
//  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling
//  coalescedRDD.toDF.write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/stocks15")

  /**
   * Exercises
   *
   * 1. Read the movies.json as an RDD.
   * 2. Show the distinct genres as an RDD.
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.
   * 4. Show the average rating of movies by genre.
   */

  //1
  case class MoviesValue(Title: String, US_Gross: Long, Worldwide_Gross: Long, US_DVD_Sales: Long,
                         Production_Budget: Long, Release_Date: String, MPAA_Rating: String,
                         Running_Time_min: Long, Distributor: String, Source: String, Major_Genre: String,
                         Creative_Type: String, Director: String, Rotten_Tomatoes_Rating: Double,
                         IMDB_Rating: Double, IMDB_Votes: Long)
  val moviesRDD2 = sc.textFile("src/main/resources/data/movies.json")
    .map(line => line.split(","))
    .map(tokens => MoviesValue(tokens(0), tokens(1).toLong, tokens(2).toLong,
      tokens(3).toLong, tokens(4).toLong, tokens(5), tokens(6),
      tokens(7).toLong, tokens(8), tokens(9), tokens(10), tokens(11), tokens(12),
      tokens(13).toDouble, tokens(14).toDouble, tokens(15).toLong))

  //2
  val rddDist = moviesRDD2.map(_.Major_Genre).distinct()
  rddDist.collect().foreach(println)

  //3
  val drama6RDD = moviesRDD2.filter(row => row.Major_Genre == "Drama" && row.IMDB_Rating > 6)

}
