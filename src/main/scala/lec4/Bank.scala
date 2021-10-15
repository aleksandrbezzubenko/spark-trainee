package lec4

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Bank extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("DataFrames Exercises")
    .config("spark.master", "local")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val eventDF = spark.read
    .option("inferSchema", "true")
    .parquet("spark-cluster/data/exvent")

  val ext_factDF = spark.read
    .option("inferSchema", "true")
    .parquet("spark-cluster/data/ext_fact")

  val resolutionsDF = spark.read
    .option("inferSchema", "true")
    .parquet("spark-cluster/data/resolutions")

  val firstDay = args(0)
  val lastDay = args(1)

  val eventDtDF = eventDF.filter((col("event_dt") > firstDay)
    and (col("event_dt") < lastDay))

  val extFactDtDF = ext_factDF.filter((col("event_dt") > firstDay)
    and (col("event_dt") < lastDay))

  val resolutionsDtDF = resolutionsDF.filter((col("event_dt") > firstDay)
    and (col("event_dt") < lastDay))

  val recipientsDF = extFactDtDF.groupBy("user_id").agg(
    collect_list("cards_payee_card_number").as("getters"))

  import scala.collection.mutable.Map
  // counting the number of occurrences of each element
  def countOfEachElement(list: Seq[String]): Map[String, Int] = {
    if (list.nonEmpty) {
      var map: Map[String, Int] = Map()
      for (i <- list.indices) {
        if (!map.contains(list(i))) {
          map += (list(i) -> 1)
        } else {
          map(list(i)) = map(list(i)) + 1
        }
      }
      map
    } else {Map("null" -> 0)}
  }

  import spark.implicits._
  // getting a column with recipients and the number of transactions to them for each user
  val recipientsAndCount = recipientsDF.map(row=>{
    val recipientsAndCount = countOfEachElement(row.getSeq[String](1))
    (row.getString(0), recipientsAndCount)
  })
  val recipientsAndCountDF = recipientsAndCount.toDF("user_id", "recipients_and_count")

  // sum of all user transactions
  val sumTransactionAmountDF = eventDtDF.groupBy(col("user_id"))
    .agg(sum("transaction_amount").as("sum_transaction_amount"))

  // search for last flag resolution of transactions
  val windowSpec = Window.partitionBy("event_id").orderBy("created")
  val resTmpDF = resolutionsDtDF.withColumn("lead",lead("resolution",1).over(windowSpec))
  val lastResolutions = resTmpDF.filter(col("lead").isNull).drop("lead")

  val joinDF = eventDtDF
    .select("event_id", "user_id")
    .join(lastResolutions.select("event_id", "resolution"),
      eventDtDF.col("event_id") === lastResolutions.col("event_id"), "leftouter")

  // search for legal transactions
  val legalTransactionsDF = joinDF.filter((col("resolution") === 'U')
    or (col("resolution") === 'A') or (col("resolution") === 'G'))
  val legalTransactionsCountDF = legalTransactionsDF.groupBy("user_id")
    .count.as("legal_transactions_count")

  // search for illegal transactions
  val illegalTransactionsDF = joinDF.filter((col("resolution") === 'F')
    or (col("resolution") === 'S'))
  val illegalTransactionsCountDF = illegalTransactionsDF.groupBy("user_id")
    .count.as("illegal_transactions_count")

  // consolidation of intermediate results
  val sumAndLegal = sumTransactionAmountDF.join(legalTransactionsCountDF, sumTransactionAmountDF.col("user_id")
    === legalTransactionsCountDF("user_id"), "left_outer")
  val sumAndLegalDF = sumAndLegal.toDF("user_id", "sum_transaction_amount", "to_delete", "legal_transactions_count")
    .drop("to_delete")
  val sumAndLegalAndIllegal = sumAndLegalDF.join(illegalTransactionsCountDF, sumAndLegalDF.col("user_id")
    === illegalTransactionsDF("user_id"), "left_outer")
  val sumAndLegalAndIllegalDF = sumAndLegalAndIllegal.toDF("user_id", "sum_transaction_amount", "legal_transactions_count",
    "to_delete", "illegal_transactions_count").drop("to_delete")
    .na.fill(1, Seq("legal_transactions_count"))
    .na.fill(0, Seq("illegal_transactions_count"))
  val result = sumAndLegalAndIllegalDF.join(recipientsAndCountDF, sumAndLegalAndIllegalDF.col("user_id")
    === recipientsAndCountDF("user_id"), "left_outer")
  val resultDF = result.toDF("user_id", "sum_transaction_amount", "legal_transactions_count",
    "illegal_transactions_count", "to_delete", "recipients_and_count").drop("to_delete")

  // writing result
  resultDF.write
    .mode(SaveMode.Overwrite)
    .save("spark-cluster/data/result.parquet")

}
