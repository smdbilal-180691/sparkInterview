package com.bilal.spark_postgres
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SaveMode

object PostgreConn {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().master("local").appName("Postgres").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val jdbcDF = spark.read.format("jdbc")
                  .option("url","jdbc:postgresql://localhost:5432/test" )
                  .option("dbtable","\"first\"")
                  .option("user","postgres")
                  .option("password","Allah.,123")
                  .load()
        jdbcDF.show
        jdbcDF.printSchema
        
    jdbcDF.write.format("jdbc").mode(SaveMode.Append)
                  .option("url","jdbc:postgresql://localhost:5432/test" )
                  .option("dbtable","\"first_out\"")
                  .option("user","postgres")
                  .option("password","Allah.,123")
                  .save()
                  
        println("Data loaded successfully")
        
  }
}