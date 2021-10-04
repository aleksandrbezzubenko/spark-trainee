package lec1

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object Exercises extends App {

  System.setProperty("hadoop.home.dir", "C:\\hadoop")

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

//  // create DF from tuples
//  val smartphones = Seq(
//    ("iphone", "5s", 4, 8),
//    ("samsung", "galaxy s5", 5, 12),
//    ("xiaomi", "redmi 5a", 5, 8),
//    ("iphone", "6s", 5, 12),
//    ("realme", "6s", 6, 48),
//    ("sony", "xperia Z3", 5, 21),
//    ("samsung", "galaxy a5", 5, 12),
//    ("iphone", "7", 5, 12),
//    ("nokia", "3310", 2, 0),
//    ("iphone", "X", 6, 12)
//  )
//
  import spark.implicits._
//
//  val manualSmartphonesDFWithImplicits = smartphones.toDF("Make", "Model", "Screen dimension", "Camera megapixels")
//
//  manualSmartphonesDFWithImplicits.show()

  // 2

//  val movies = spark.read
//    .option("inferSchema", "true")
//    .json("src/main/resources/data/movies.json")
//
//  movies.printSchema()
//  println(movies.count())



  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file CSV \t
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB / json
   */

  val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

  //csv
//  moviesDF.coalesce(1).write
//    .mode(SaveMode.Overwrite)
//    .option("sep", "a/ta")
//    .csv("src/main/resources/data/movies_duplicate")

  //parquet
//  moviesDF.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/movies_parquet")

  //json table
//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .format("json")
//    .save("src/main/resources/data/movies_jstest.json")


  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */

//  //1
//  moviesDF.select($"Title", $"US_Gross").show

  //2
//  moviesDF.withColumn("Total_profit", col("US_Gross")
//    + col("Worldwide_Gross") + col("US_DVD_Sales")).show

  //3
//  moviesDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show

  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have(Director)
   * 3. Show the mean and standard deviation(stddev) of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */
  //1
//  moviesDF.withColumn("Total_profit", col("US_Gross")
//    + col("Worldwide_Gross") + col("US_DVD_Sales")).select(sum(col("Total_profit"))).show

  //2
//  println(moviesDF.select("Director").distinct().count)

  //3
//  moviesDF.agg(
//    avg(col("US_Gross")),
//    stddev(col("US_Gross"))
//  ).show

  //4
//  moviesDF.groupBy("Director").agg(
//    avg("IMDB_Rating"),
//    avg("US_Gross")
//  ).show

  /**
   * Exercises
   *
   * 1. show all employees and their max salary
   *    (employees from j and salaries form joins/salaries)
   *    note that in salaries may be moins/employeesore than 1 salary for employee -> take the biggest(before join make a group by on salaries)
   * 2. show all employees who were never managers
   *    employees from joins/employees and info about managers from joins/dept_manager.
   *    Show all employees that are not exist in table joins/dept_manager
   * 3. find the job titles of the best paid 10 employees in the company
   *    title from joins/titles take the latest title(may need to group by and max by to_date)
   */

  //1
//  val employeesDF = spark.read
//    .option("inferSchema", "true")
//    .parquet("src/main/resources/data/joins/employees")
//
//  val salariesDF = spark.read
//    .option("inferSchema", "true")
//    .parquet("src/main/resources/data/joins/salaries")
//
//  val deptManagerDF = spark.read
//    .option("inferSchema", "true")
//    .parquet("src/main/resources/data/joins/dept_manager")
//
//  val titlesDF = spark.read
//    .option("inferSchema", "true")
//    .parquet("src/main/resources/data/joins/titles")

  //1
//  val maxSalaryDF = salariesDF
//    .groupBy("emp_no")
//    .max("salary")
//
//  employeesDF.join(maxSalaryDF, (maxSalaryDF("emp_no") === employeesDF("emp_no")),"left_outer").show

  //2
//  val menegersDF = deptManagerDF.select("emp_no")
//  employeesDF.join(menegersDF, (employeesDF("emp_no") === menegersDF("emp_no")), "anti").show

  //3
//  val best10EmployeesDF = maxSalaryDF.sort($"max(salary)".desc).limit(10)
//  employeesDF.join(best10EmployeesDF, (employeesDF("emp_no") === best10EmployeesDF("emp_no")), "semi").show

}
