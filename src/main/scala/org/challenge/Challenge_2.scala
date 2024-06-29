package org.challenge

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Challenge_2{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Challenge_2")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .csv("data/googleplaystore.csv")

    val df_no_nan = df.withColumn("Rating",when(isnan(col("Rating")),lit(0)).otherwise(col("Rating")))


    val df_2 = df_no_nan.where(col("Rating") >= 4.0)

    //Assuming the descending order is referent to the Rating and not the name of the App
    df_2.orderBy(desc("Rating")).show()

    df_2.coalesce(1).write
      .option("delimiter", "§")
      .option("header", "true")
      .csv("data/best_apps.csv")

    spark.stop()
  }
}