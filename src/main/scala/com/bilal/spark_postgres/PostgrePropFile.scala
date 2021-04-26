package com.bilal.spark_postgres

import org.apache.spark.sql.SparkSession
import java.io._
import java.util.Properties
import org.apache.spark.sql.functions.spark_partition_id

object PostgrePropFile {
  def main(args:Array[String]):Unit={
    
    val spark= SparkSession.builder().master("local").appName("PostgresWithPropertyFile").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val reader = new FileInputStream("input_data\\properties\\postgre.properties")
    
    val connectionProperties = new Properties()
    connectionProperties.load(reader)
    
    val url = connectionProperties.getProperty("url")
    val table = connectionProperties.getProperty("table")
    val columnName = connectionProperties.getProperty("columnName")
    val lowerBound = connectionProperties.getProperty("lowerBound").toLong
    val upperBound = connectionProperties.getProperty("upperBound").toLong
    val numPartitions = connectionProperties.getProperty("numPartitions").toInt
    
    val in = spark.read.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties)
    
    in.show(false)
    in.select("*").withColumn("part_id", spark_partition_id()).show(false)
    in.printSchema
    
   
    
  }
}