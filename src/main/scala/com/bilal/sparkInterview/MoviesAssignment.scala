package com.bilal.sparkInterview
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row,SaveMode}
/**
 * Req1
 * Create a CSV file containing list of unique Genres and number of movies under each genres.
 *  The file should contain two columns i.e, Genres, No of movies. 
 */

/**
 * Req2
 * Create a CSV file containing list of movies with number of users who rated the movie and average rating per movie. 
 * The file has to with three columns, i.e, MovieId, No of users, Average rating. 
 */
object MoviesAssignment {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("MovieGenresCount").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    println("#####################Requirement 1######################################")
    val movies_schema = StructType(
        Seq(StructField("movieid",StringType),
            StructField("title",StringType),
            StructField("genres",StringType)))
    val movies_df = spark.read.schema(movies_schema).option("sep","~").csv("input_data\\movies")
    
    val movies_df2 = movies_df.withColumn("genres", split(col("genres"), "\\|") )
                  .withColumn("genres", explode(col("genres")))
                  
    println("Input Sample after explode")
    movies_df2.show(5,false)
    
    println("Output data showing the genres and number of movies")
    val movies_df3 = movies_df2.select("genres","title").groupBy("genres")
                .agg(count("title").alias("count")).drop("title")
    movies_df3.show()
    val genre_write = movies_df3.coalesce(1).write.mode(SaveMode.Append).csv("output_data\\movie_genre_analytics")
    
    println("############################################################################")
    
    println("#########################Requirement 2#####################################")
    val ratings_schema = StructType(
                          Seq(StructField("userid",StringType),
                              StructField("movieid",StringType),
                              StructField("rating",StringType)
                              )
                        )
    var ratings_df = spark.read.text("input_data\\movie_ratings")
    val ratings_rdd = ratings_df.rdd.map(col => col.mkString.split("\\:\\:")).map(i=>Row.fromSeq(i))
    
    ratings_df = spark.createDataFrame(ratings_rdd, ratings_schema) 
    val ratings_output = ratings_df.groupBy("movieid").agg(count("userid").alias("no_of_users_rated")
                      ,round(sum("rating").divide(count("userid")),2).alias("avg_rating")).drop("userid")
                      .withColumn("movieid",col("movieid").cast(IntegerType))
                      .sort("movieid")
    ratings_output.show(10)
    
    val ratings_write = ratings_output.coalesce(1).write.mode(SaveMode.Append).csv("output_data\\movie_rating_analytics")
    
    val val_count = ratings_output.drop("movieid","avg_rating").agg(sum("no_of_users_rated")).collect().mkString
    print(val_count)
  }
}