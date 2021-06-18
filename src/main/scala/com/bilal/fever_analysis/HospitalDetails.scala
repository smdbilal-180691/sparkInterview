package com.bilal.fever_analysis
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SaveMode
import java.nio.file.{Paths, Files}
import java.io.File

object HospitalDetails {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().master("local").appName("Hospital Details").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val hospitalsDF = spark.read.format("jdbc")
                  .option("url","jdbc:postgresql://localhost:5432/Idris" )
                  .option("dbtable","\"hospitals\"")
                  .option("user","postgres")
                  .option("password","Allah.,123")
                  .load().orderBy("date")
              hospitalsDF.show(false)    
              
    val directory = new File("G:\\iddu_data_lake\\hospital_details")
    val list = directory.listFiles().toList
    if(list.length == 0)
    {
      val datalake_df = hospitalsDF.coalesce(1).write.option("header", true).mode(SaveMode.Append)
                      .csv("G:\\iddu_data_lake\\hospital_details")
    }
    else{
      val existing_df = spark.read.option("header","true").csv("G:\\iddu_data_lake\\hospital_details")
                      .select("date")
    
   
    val incremental_df = hospitalsDF.join(existing_df,hospitalsDF("date")===existing_df("date"),"left_anti")
    
      if(incremental_df.limit(1).count() > 0){
              
        val datalake_df = incremental_df.coalesce(1).write.option("header", true).mode(SaveMode.Append)
                      .csv("G:\\iddu_data_lake\\hospital_details")
      }
      else{
        println("No data to write")
      }
    }
     
    
                      
                  
  }
}