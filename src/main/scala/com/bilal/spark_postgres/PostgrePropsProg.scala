package com.bilal.spark_postgres
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions.spark_partition_id
import java.util.Properties

object PostgrePropsProg {
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
        
        jdbcDF.groupBy(spark_partition_id()).count().show()
        
    val connectionProperties = new Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password","Allah.,123")
    
    val meth2=    spark.read.jdbc("jdbc:postgresql://localhost:5432/test", "first",
              "id", 1L, 9L, 3,connectionProperties)
              
    meth2.show
    meth2.orderBy(spark_partition_id()).groupBy(spark_partition_id()).count().show()
    meth2.select("*").withColumn("part_id", spark_partition_id()).show
    
    spark.read.option("basePath", "G:\\nifi_postgres_output").csv("G:\\nifi_postgres_output\\occupation=*").show
        
  }
}