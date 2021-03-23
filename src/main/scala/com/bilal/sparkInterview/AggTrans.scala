package com.bilal.sparkInterview
/**
 * This is used to perform sum of transaction amount made by every customer and round off to two decimal places
 * Two methods are discussed here
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object AggTrans {
  def main(args:Array[String]):Unit={
    
    val spark = SparkSession.builder().master("local").appName("aggegate").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    
    val trans_df = spark.read.option("header", true)
                    .csv("input_data\\agg_trans") 
    
//Method 1 is to get pivot of credit and debit transactions sum them and get diff of credit and sum
    println("Method 1 Output")               
    val pivot_df = trans_df.groupBy(trans_df("Customer_No")).pivot("Transaction Type").agg(sum("Amount"))  
    val out_df = pivot_df
                 .withColumn("total_bal", round((col("credit") - col("debit")),2)).drop("credit","debit").show
    
//Method 2 is multiple debit by -1 and find the sum of transactions for customer number 
    println("Method 2 output")
    trans_df
    .withColumn("Amount", when(col("Transaction Type").equalTo("debit"),lit(-1)*col("Amount"))
    .otherwise(col("Amount"))).groupBy("Customer_No").agg(round(sum("Amount"),2).alias("total_bal")).show
    
  }
}