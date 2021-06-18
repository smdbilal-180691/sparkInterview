package com.bilal.fever_analysis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._
object FeverDetails {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().master("local").appName("Fever Details").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    

     val fever_df = spark.read.format("jdbc")
                    .option("url", "jdbc:postgresql://localhost:5432/Idris")
                    .option("query", " select * from fever_readings where date(date_time) = current_date")
                    .option("user", "postgres")
                    .option("password", "Allah.,123")
                    .load
                    .withColumn("dt", to_date(col("date_time")))
    
         fever_df.show
    
  }
}