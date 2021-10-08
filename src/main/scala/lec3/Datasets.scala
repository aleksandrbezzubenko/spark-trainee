package lec3

import java.sql.Date
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.math.Ordered.orderingToOrdered

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ > 12)

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String, //with Date there was an error: cannot convert date to string
                  Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")

  // 3 - define an encoder (importing the implicits)
  import spark.implicits._
  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  carsDS.filter(_.Origin.contains("1"))

  // map, flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  /**
   * Exercises
   *
   * 1. Count how many cars we have
   * 2. Count how many POWERFUL cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */
  //1
  println(carsDS.count)

  //2
  println(carsDS.filter(_.Horsepower > Some(140)).count)

  //3
  carsDS.select(avg(col("Horsepower"))).show


  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")


  /**
   * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
   * (hint: use array_contains)
   */

  val guitarPlayerGuitarsDS: Dataset[(GuitarPlayer, Guitar)] = guitarPlayersDS.joinWith(guitarsDS,
    array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .withColumnRenamed("_1", "GuitarsPlayers").withColumnRenamed("_2", "Guitars").as[(GuitarPlayer, Guitar)]
  guitarPlayerGuitarsDS.show(false)

}
