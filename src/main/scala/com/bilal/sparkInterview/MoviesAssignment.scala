package com.bilal.sparkInterview
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
/**
 * Create a CSV file containing list of unique Genres and number of movies under each genres.
 *  The file should contain two columns i.e, Genres, No of movies. Column headers are not required
 */
object MoviesAssignment {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("MovieGenresCount").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sch = StructType(
        Seq(StructField("movieid",StringType),
            StructField("title",StringType),
            StructField("genres",StringType)))
    val in_df = spark.read.schema(sch).option("sep","~").csv("input_data\\movies")
    
    val in_df2 = in_df.withColumn("genres", split(col("genres"), "\\|") )
                  .withColumn("genres", explode(col("genres")))
                  
    println("Input Sample after explode")
    in_df2.show(5,false)
    
    println("Output data showing the genres and number of movies")
    in_df2.select("genres","title").groupBy("genres").agg(count("title").alias("count")).drop("title").show(false)
    
    
    
  }
}