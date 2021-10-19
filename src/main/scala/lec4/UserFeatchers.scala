import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, lead, sum, udf, when}
import scala.collection.mutable

object UserFeatchers {

  def start(lowerBound: String, upperBound: String, path: String): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DataFrames Exercises")
      .config("spark.master", "local")
      //.config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventDtCol = col("event_dt")
    val dataFilter = (eventDtCol > lowerBound) and (eventDtCol < upperBound)


    val eventDtDF = spark.read
      .parquet(s"${path}/exvent")
      .filter(dataFilter)
      .select("event_id", "transaction_amount")

    val extFactDtDF = spark.read
      .parquet(s"${path}/ext_fact")
      .filter(dataFilter)
      .select("user_id", "cards_payee_card_number", "event_id")

    val resolutionsDtDF = spark.read
      .parquet(s"${path}/resolutions")
      .filter(dataFilter)
      .select("event_id", "resolution", "created")

    val windowSpec = Window.partitionBy("event_id").orderBy("created")
    val lastResolutions = resolutionsDtDF
      .withColumn("lead",
        lead("resolution",1).over(windowSpec))
      .filter(col("lead").isNull).drop("lead")

    val gettersCount = udf { arr: mutable.WrappedArray[String] =>
      arr.distinct.map(e => e -> arr.count(_ == e)).toMap
    }

    val resultDF = eventDtDF.join(
      extFactDtDF,
      Seq("event_id"),
      "left"
    ).join(
      lastResolutions,
      Seq("event_id"),
      "left"
    ).withColumn("is_fraud", col("resolution")
      .isin("F","S") or col("resolution").isNull)
      .groupBy(col("user_id"))
      .agg(
        sum("transaction_amount").as("sum_transaction_amount"),
        sum(when(col("is_fraud"), 1).otherwise(0)).as("count_legal"),
        sum(when(!col("is_fraud"), 1).otherwise(0)).as("count_fraud"),
        collect_list("cards_payee_card_number").as("getters")
      ).withColumn("getters_count", gettersCount(col("getters")))

     // writing result
    resultDF.write
      .mode(SaveMode.Overwrite)
      .save(s"${path}/result")
  }
}
