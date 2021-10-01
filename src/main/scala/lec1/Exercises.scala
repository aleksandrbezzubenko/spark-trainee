package lec1

import lec1.DataSources.{driver, password, url, user}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, countDistinct, expr, max, mean, stddev, sum}

object Exercises {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

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

  // 1
  val smartphones = Seq(
    ("Samsung", "Galaxy S10", "Android", 12),
    ("Apple", "iPhone X", "iOS", 13),
    ("Nokia", "3310", "THE BEST", 0)
  )

  import spark.implicits._

  val smartphonesDF = smartphones.toDF("Make", "Model", "Platform", "CameraMegapixels")
  smartphonesDF.show()

  // 2
  var moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(s"The Movies DF has ${moviesDF.count()} rows")



  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB
   */

  val moviesDFEx2 = spark.read.json("src/main/resources/data/movies.json")

  // TSV
  moviesDFEx2.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  // Parquet
  moviesDFEx2.write.save("src/main/resources/data/movies.parquet")

  // save to DF
  moviesDFEx2.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()

  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */

  moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  moviesDF.show()

  // 1
  val moviesReleaseDF = moviesDF.select("Title", "Release_Date")
  val moviesReleaseDF2 = moviesDF.select(
    moviesDF.col("Title"),
    col("Release_Date"),
    $"Major_Genre",
    expr("IMDB_Rating")
  )
  val moviesReleaseDF3 = moviesDF.selectExpr(
    "Title", "Release_Date"
  )

  // 2
  val moviesProfitDF = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )

  val moviesProfitDF2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  val moviesProfitDF3 = moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  // 3
  val atLeastMediocreComediesDF = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  val comediesDF2 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  val comediesDF3 = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  comediesDF3.show


  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */


  // 1
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()

  // 2
  moviesDF
    .select(countDistinct(col("Director")))
    .show()

  // 3
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  // 4
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()

  /**
   * Exercises
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job titles of the best paid 10 employees in the company
   */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/scala_essential"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")

  // 2
  val empNeverManagersDF = employeesDF.join(
    deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti"
  )

  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()
}
