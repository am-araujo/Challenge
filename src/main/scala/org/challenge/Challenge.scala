package org.challenge

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Challenge{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Challenge")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .csv("data/googleplaystore_user_reviews.csv")

    val df_no_nan = df.withColumn("Sentiment_Polarity",when(isnan(col("Sentiment_Polarity")),lit(0)).otherwise(col("Sentiment_Polarity")))

    val df_1 = df_no_nan.groupBy("App").agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    df_1.printSchema()
    df_1.show()

    spark.stop()
  }
}