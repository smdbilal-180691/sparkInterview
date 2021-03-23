package com.bilal.sparkInterview
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
object jsonColHandling {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("json handling").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
       
    val in_df = spark
                .read
                .option("header", true)
                .option("escape", "\"")
                .option("multiLine",true)
                .option("sep", "|")
                .csv("G:\\Spark_Interview_inputs\\json_col_handling")
    
    println("The input df is ")
    in_df.show(false)
    in_df.printSchema()
    
    val json_schema = StructType(Seq(StructField("MessageId",StringType),StructField("Latitude",StringType),
                      StructField("Longitude",StringType)))
    
    println(in_df.select("request").schema)
    
    def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ => Array(col(columnName).as(columnName.replace(".","_")))
      }
    })
  }
    
    
    /**
     * Method 1: 
     * Get the nested json column as normal json using json_tuple
     * Use from_json and attach a schema to it.. and select that column using column.*
     */
    println("*******************************************************************")
    println("***************************Method 1********************************")
    println("*******************************************************************")
    val tuple_df = in_df.withColumn("request", json_tuple($"request", "Response"))   
    val json_df = tuple_df.withColumn("request", from_json($"request", json_schema))
    json_df.show(false)
    json_df.select("*","request.*").drop("request").show(false)
    
    /**
     * Method 2: U dont have to know about the column names
     * Convert the json col into rdd and get the schema
     * Use the schema in from_json method on that json column refering the original df
     * Now that json column will become Struct column
     * Flatten the entire schema and you will get the required result
     */
    
    println("*******************************************************************")
    println("***************************Method 2********************************")
    println("*******************************************************************")
    val x = in_df.select("request").alias("jsonCol").rdd.map(_.get(0)).collect().toList.map(_.toString())
    val y = spark.sparkContext.parallelize(x)
    val x_sch = spark.read.json(y).schema
    println(x_sch)
    val json_df1 = in_df.withColumn("request",from_json($"request", x_sch))
    
    println(flattenStructSchema(json_df1.schema).mkString)
    
    json_df1.select(flattenStructSchema(json_df1.schema):_*).show(false)    
    
  }
}