package lec2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object LecWorkflow extends App {
  val spark = SparkSession.builder()
    .appName("Basic Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  val str = "sad" * 1000
  moviesDF.withColumn("static_col", lit(str)).select("static_col").show()

  // Boolean
  val booleanCol = (col("IMDB_Rating") > 5) or (col("Rotten_Tomatoes_Rating") > 6)
  moviesDF.withColumn("static_col", booleanCol).select("static_col").show()

  moviesDF.filter(booleanCol).show()

  // In aggregate
  moviesDF.groupBy("Director")
    .agg(
      max(when(booleanCol, col("IMDB_Rating")).otherwise(null))
    )

  // Numeric
  moviesDF.select(col("Production_Budget") + 1,
    col("Production_Budget") / 2,
    col("Production_Budget") % 15
  )

  // String
  moviesDF.select(
    col("Major_Genre") + "a",
    col("Major_Genre") contains "a", // if 'a' in string -> true
    lower(col("Major_Genre")), // ABC -> abc
    upper(col("Major_Genre"))
  )

  // Complex types
  val seq = Seq(1, 2, 3, 4, 5)
  val dfWithSeq = Seq(
    (seq, Map("1" -> 12, "13" -> 13)),
    (seq, Map("1" -> 12, "14" -> 13))
  ).toDF("a", "b")
  dfWithSeq.printSchema()
  dfWithSeq.show()
  dfWithSeq.select(col("a").getItem(1)).show()
  dfWithSeq.select(col("b").getItem(1))

  // Complex type
  case class User(name: String, birthday: String, age: Int)
  val schema = StructType(
    Array(
      StructField(
        "user", StructType(
          Seq(
            StructField("name", StringType),
            StructField("birthday", StringType),
            StructField("age", IntegerType)
          )
        )
      )
    )
  )
  val df = Seq(
    (User("Ivan", "13.01.1974", 47), 1),
    (User("Fedor", "13.01.2001", 20), 2)
  ).toDF("user", "fake")
  df.printSchema()
  df.show()
  df.select(col("user.age").as("user_age"), col("user.name").as("user_name")).show()

  // Managing nulls

  val dfWithNulls = Seq(
    ("abc", Option(123), Some(true)),
    ("def", None, Some(false)),
    (null, Option(213), None)
  ).toDF("str", "int", "bool")
  dfWithNulls.show()
  dfWithNulls.printSchema()
  dfWithNulls.select(
    $"int" * 10,
    $"str".contains("b"),
    $"bool" || lit(true),
    $"bool" or lit(false),
    $"bool" && lit(false),
    $"bool" && lit(true)
  ).show()

  // check if null
  dfWithNulls.select($"int" === null, $"int" =!= null, $"int".isNull, $"int".isNotNull).show()

  // 1
  dfWithNulls.na.drop(Seq("int")).show()
  dfWithNulls.na.fill(0, Seq("int")).show()

  // 2
  dfWithNulls.select(when($"int".isNull, 0).otherwise($"int"))
}
