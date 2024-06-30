package org.challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Challenge{

  //Retrieve record where the highest number of reviews happened, for each App
  private def max_reviewed_record(df: DataFrame)= {
    val window = Window.partitionBy("App")


    val df_max_review = df.withColumn("maxReviews", max("Reviews").over(window))
      .where(col("Reviews").equalTo(col("maxReviews")))
      .drop("maxReviews").drop("Category")
    df_max_review
  }

  private def columns_renamed(df: DataFrame) = {
    //Renaming columns
    val column_name_changes: Map[String, String] = Map.apply("Content Rating" -> "Content_Rating",
      "Last Updated" -> "Last_Updated",
      "Current Ver" -> "Current_Version",
      "Android Ver" -> "Minimum_Android_Version")

    val df_renamed = df.withColumnsRenamed(column_name_changes)
    df_renamed
  }

  private def remove_duplicates_categorized(df: DataFrame) = {
    //Removing duplicate app names and categories
    val no_dup = df.dropDuplicates("App", "Category")

    //Concatenating different categories
    val categorized = no_dup.groupBy("App").agg(collect_list("Category").alias("Categories"))
    categorized
  }

  private def csv_reader(spark: SparkSession, path: String) = {
    val df = spark.read
      .option("header", value = true)
      .csv(path)
    df
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Challenge")
      .getOrCreate()

    //Path to googleplaystore.csv file
    val google_playstore_path = "data/googleplaystore.csv"

    //Path to googleplaystore_user_reviews.csv file
    val user_reviews_path = "data/googleplaystore_user_reviews.csv"

    val df_user_reviews: DataFrame = csv_reader(spark, user_reviews_path)

    val df: DataFrame = csv_reader(spark, google_playstore_path)

    val df_no_nan = df_user_reviews.withColumn("Sentiment_Polarity",when(isnan(col("Sentiment_Polarity")),lit(0)).otherwise(col("Sentiment_Polarity")))

    val df_1 = df_no_nan.groupBy("App").agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    val df_no_nan_rating = df.withColumn("Rating",when(isnan(col("Rating")),lit(0)).otherwise(col("Rating")))

    var df_2 = df_no_nan_rating.where(col("Rating") >= 4.0)

    //Assuming the descending order is referent to the Rating and not the name of the App
    df_2 = df_2.orderBy(desc("Rating"))

//    df_2.coalesce(1).write
//      .option("delimiter", "§")
//      .option("header", "true")
//      .csv("data/best_apps.csv")

    val df_max_review: DataFrame = max_reviewed_record(df)

    val categorized: DataFrame = remove_duplicates_categorized(df)

    val final_df = categorized.join(df_max_review, Seq("App"))

    val df_renamed: DataFrame = columns_renamed(final_df)

    //Removing M from column Size
    val df_size_updated = df_renamed.withColumn("Size", when(col("Size").like("%M"), regexp_replace(col("Size"), "M", ""))
      .otherwise(col("Size")))

    //Converting kb to MB
    val df_k_to_M = df_size_updated.withColumn("Size", when(col("Size").like("%k"), regexp_replace(col("Size"), "k", "")
      .cast("double") / lit(1024)).otherwise(col("Size").cast("double")))

    //Converting from String to Double
    val rating_converted = df_k_to_M.withColumn("Rating", when(!isnan(col("Rating")),col("Rating").cast("double")))

    //Converting from String to Date (Assuming data type is Date and not timestamp like example table)
    val date_converted = rating_converted.withColumn("Last_Updated", to_date(col("Last_Updated"), "MMMM d, y"))

    //Converting from String to Date (Assuming data type is Date and not timestamp like example table)
    val reviews_converted = date_converted.withColumn("Reviews", coalesce(col("Reviews").cast("long"), lit(0)))

    val genres_converted = reviews_converted.withColumn("Genres", split(col("Genres"), ";"))

    val df_3 = genres_converted.withColumn("Price", regexp_replace(col("Price"), "[$,]", "").cast("double") * lit(0.9))

    val df_3_1 = df_3.join(df_1, Seq("App"))

    df_3_1.write.option("compression", "gzip").parquet("googleplaystore_cleaned")

    val df_3_exploded = df_3.withColumn("Genre", explode(col("Genres")))

    val sentiment_df = df_no_nan.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double")).select("App", "Sentiment_Polarity")

    val genre_sentiment = df_3_exploded.join(sentiment_df, Seq("App"))

    val genre_sentiment_average = genre_sentiment.groupBy("Genre").agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    val count_average_rating = df_3_exploded.groupBy("Genre")
      .agg(count("Genre").alias("Count"),
          avg("Rating").alias("Average_Rating"))

    val df_4 = count_average_rating.join(genre_sentiment_average, Seq("Genre"))

    df_4.write.option("compression", "gzip").parquet("googleplaystore_metrics")

    spark.stop()
  }


}
