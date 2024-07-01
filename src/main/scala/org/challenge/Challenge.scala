package org.challenge

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Challenge{
  //Auxiliary function for reading csv files
  private def csv_reader(spark: SparkSession, path: String) = {
    val df = spark.read
      .option("header", value = true)
      .csv(path)
    df
  }

  //Change NaN values to 0 in column with "column_name"
  private def remove_nan(df_user_reviews: DataFrame, column_name: String) = {
    val df_no_nan = df_user_reviews.withColumn(column_name, when(isnan(col(column_name)), lit(0)).otherwise(col(column_name)))
    df_no_nan
  }

  //Change NULL values to 0 in column with "column_name"
  private def remove_null(df_user_reviews: DataFrame, column_name: String) = {
    val df_no_null = df_user_reviews.withColumn(column_name, when(isnull(col(column_name)), lit(0)).otherwise(col(column_name)))
    df_no_null
  }

  //Get Apps rated 4.0 or higher and sort them from best to worse rating
  private def get_best_apps_sorted(df_no_nan_rating: DataFrame) = {
    //Filter Dataframe by only retrieving Apps with Rating above 4.0
    val best_ratings = df_no_nan_rating.where(col("Rating") >= 4.0)

    //Assuming the descending order is referent to the Rating and not the name of the App
    val df_2 = best_ratings.orderBy(desc("Rating"))
    df_2
  }

  //Retrieve record where the highest number of reviews happened, for each App
  private def max_reviewed_record(df: DataFrame)= {
    val window = Window.partitionBy("App")

    val df_max_review = df.withColumn("maxReviews", max("Reviews").over(window))
      .where(col("Reviews").equalTo(col("maxReviews")))
      .drop("maxReviews").drop("Category")
    df_max_review
  }

  //Get a new Dataframe with Categories now appearing in list form
  private def remove_duplicates_categorized(df: DataFrame) = {
    //Removing duplicate app names and categories
    val no_dup = df.dropDuplicates("App", "Category")

    //Concatenating different categories
    val categorized = no_dup.groupBy("App").agg(collect_list("Category").alias("Categories"))
    categorized
  }

  //Auxiliary function for renaming columns
  private def columns_renamed(df: DataFrame) = {
    //Map with the old and new column names
    val column_name_changes: Map[String, String] = Map.apply("Content Rating" -> "Content_Rating",
      "Last Updated" -> "Last_Updated",
      "Current Ver" -> "Current_Version",
      "Android Ver" -> "Minimum_Android_Version")

    //Dataframe updated with new names
    val df_renamed = df.withColumnsRenamed(column_name_changes)
    df_renamed
  }

  //Converting from kilobytes to MB and casting Size column to Double afterwards
  private def converting_size(df_renamed: DataFrame) = {
    //Removing M from column Size
    val df_size_updated = df_renamed.withColumn("Size", when(col("Size").like("%M"), regexp_replace(col("Size"), "M", ""))
      .otherwise(col("Size")))

    //Converting kb to MB and casting to double
    val df_k_to_M = df_size_updated.withColumn("Size", when(col("Size").like("%k"), regexp_replace(col("Size"), "k", "")
      .cast("double") / lit(1024)).otherwise(col("Size").cast("double")))
    df_k_to_M
  }

  //Converting from String to Date (Assuming data type is Date and not timestamp like example table)
  private def date_conversion(rating_converted: DataFrame) = {
    val date_converted = rating_converted.withColumn("Last_Updated", to_date(col("Last_Updated"), "MMMM d, y"))
    date_converted
  }

  //Auxiliary function for splitting strings into array of Strings. column_to_split = Column that contains the String to split, delimiter = Delimiter used to split String
  private def string_splitter(reviews_converted: DataFrame, column_to_split : String, delimiter : String) = {
    reviews_converted.withColumn(column_to_split, split(col(column_to_split), delimiter))
  }

  // Convert column Price to Euros and cast as "Double". First remove the dollar sign, then do the conversion rate between dollars and euros
  private def price_conversion(genres_converted: DataFrame) = {
    genres_converted.withColumn("Price", regexp_replace(col("Price"), "[$,]", "").cast("double") * lit(0.9))
  }

  //Auxiliary function for splitting arrays of Strings. column_to_split = Column that contains the array, new_column = Name for new column
  private def array_splitter(df_3: DataFrame, column_to_split : String, new_column : String) = {
    df_3.withColumn(new_column, explode(col(column_to_split)))
  }

  //Get the average sentiment polarity by genre
  private def sentiment_by_genre(df_no_nan: DataFrame, df_3: DataFrame) = {
    //Splitting the array of strings previously created for Genres to retrieve all Genres available
    val df_3_exploded = array_splitter(df_3, "Genres", "Genre")

    //Casting the Sentiment Polarity column to double and retrieving the App name and the Sentiment Polarity associated with each App
    val sentiment_df = df_no_nan.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double")).select("App", "Sentiment_Polarity")

    //Joining the Genre column with the Sentiment Polarity and App Dataframe using App as join condition
    val genre_sentiment = df_3_exploded.join(sentiment_df, Seq("App"))

    //Getting the Average Sentiment Polarity by Genre
    val genre_sentiment_average = genre_sentiment.groupBy("Genre").agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
    (df_3_exploded, genre_sentiment_average)
  }

  //Get average rating by Genre
  private def count_average_by_genre(df_3_exploded: DataFrame) = {
    df_3_exploded.groupBy("Genre")
      .agg(count("Genre").alias("Count"),
        avg("Rating").alias("Average_Rating"))
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

    val df_no_nan: DataFrame = remove_nan(df_user_reviews, "Sentiment_Polarity")

    //Group by App and get the average of Sentiment Polarity
    val df_1_null_possible = df_no_nan.groupBy("App").agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    //My assumption of Default Value = 0 instead of NULL
    val df_1 = remove_null(df_1_null_possible, "Average_Sentiment_Polarity")

    val df_no_nan_rating : DataFrame = remove_nan(df, "Rating")

    //Get Apps with 4.0 or higher rating sorted
    val df_2: Dataset[Row] = get_best_apps_sorted(df_no_nan_rating)

    //Path to save new csv file
    val df_2_path = "data/best_apps.csv"

    //Save Dataframe as a csv and with "§" as delimiter
    df_2.coalesce(1).write
      .option("delimiter", "§")
      .option("header", "true")
      .csv(df_2_path)

    //Retrieve columns with max reviews except Category column
    val df_max_review: DataFrame = max_reviewed_record(df)

    val categorized: DataFrame = remove_duplicates_categorized(df)

    //Join the retrieved columns associated with the most reviews, with newly created Categories column
    val final_df = categorized.join(df_max_review, Seq("App"))

    //Change column names
    val df_renamed: DataFrame = columns_renamed(final_df)

    //Converting Size column from String to Double
    val df_k_to_M: DataFrame = converting_size(df_renamed)

    //Converting from String to Double
    val rating_converted = df_k_to_M.withColumn("Rating", when(!isnan(col("Rating")),col("Rating").cast("double")))

    //Converted from String to Date
    val date_converted: DataFrame = date_conversion(rating_converted)

    //Converting from String to Long and making sure value is not null
    val reviews_converted = date_converted.withColumn("Reviews", coalesce(col("Reviews").cast("long"), lit(0)))

    //Converting from String to array of Strings
    val genres_converted = string_splitter(reviews_converted, "Genres", ";")

    //Converting from Dollars to Euros.
    val df_3 = price_conversion(genres_converted)

    //Joining the previous Dataframe(Part 3 of exercise) with the Dataframe produced in Part 1, using App as join condition
    val df_3_1 = df_3.join(df_1, Seq("App"))

    //Path to save new parquet file
    val df_3_path = "data/googleplaystore_cleaned"

    //Saving the new Dataframe as a parquet file using gzip compression
    df_3_1.write.option("compression", "gzip").parquet(df_3_path)

    val (df_3_exploded: DataFrame, genre_sentiment_average: DataFrame) = sentiment_by_genre(df_no_nan, df_3)

    //Getting the number of apps by Genre and the Average Rating of said Genre
    val count_average_rating = count_average_by_genre(df_3_exploded)

    //Joining the last two dataframes so we get number of applications, average rating and average sentiment polarity by Genre
    val df_4 = count_average_rating.join(genre_sentiment_average, Seq("Genre"))

    //Path to save new parquet file
    val df_4_path = "data/googleplaystore_metrics"

    //Saving the new Dataframe as a parquet file using gzip compression
    df_4.write.option("compression", "gzip").parquet(df_4_path)

    spark.stop()
  }

}
