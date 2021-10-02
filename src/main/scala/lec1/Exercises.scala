package lec1

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object Exercises extends App {

  System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\hadoop")

  // Basics
  /**
   * Exercise:
   * 1) Create a manual DF describing smartphones
   *   - make
   *   - model
   *   - screen dimension
   *   - camera megapixels
   *
   * 2) Read another file from the data/ folder, e.g. movies.json
   *   - print its schema
   *   - count the number of rows, call count()
   */

  //1
  val spark: SparkSession = SparkSession.builder()
    .appName("DataFrames Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // create DF from tuples
  val smartphones = Seq(
    ("iphone", "5s", 4, 8),
    ("samsung", "galaxy s5", 5, 12),
    ("xiaomi", "redmi 5a", 5, 8),
    ("iphone", "6s", 5, 12),
    ("realme", "6s", 6, 48),
    ("sony", "xperia Z3", 5, 21),
    ("samsung", "galaxy a5", 5, 12),
    ("iphone", "7", 5, 12),
    ("nokia", "3310", 2, 0),
    ("iphone", "X", 6, 12)
  )

  import spark.implicits._

  val manualSmartphonesDFWithImplicits = smartphones.toDF("Make", "Model", "Screen dimension", "Camera megapixels")

  manualSmartphonesDFWithImplicits.show()

  // 2

  val movies = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  movies.printSchema()
  println(movies.count())



  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file CSV \t
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB / json
   */

  val moviesDF = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/movies.json")

  moviesDF.coalesce(1).write
    .mode(SaveMode.Overwrite)
    .csv("src/main/resources/data/movies_duplicate")
//    .format("com.databricks.spark.csv")
//    .option("inferSchema", "true")
//    .option("header", "true")


  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */

  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have(Director)
   * 3. Show the mean and standard deviation(stddev) of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */

  /**
   * Exercises
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job titles of the best paid 10 employees in the company
   */
}
