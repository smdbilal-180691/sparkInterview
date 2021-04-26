package com.bilal.sparkInterview
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, concat, explode, floor, lit, rand}
import org.apache.spark.sql.functions.spark_partition_id
object SaltingSkew {
  def main(args:Array[String]):Unit={
    
    val spark = SparkSession.builder().master("local").appName("skew").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    def removeDataSkew(bigLeftTable: DataFrame, Col_X: String, rightTable: DataFrame) = {
    var df1 = bigLeftTable
             .withColumn(Col_X, concat(
                         bigLeftTable.col(Col_X), lit("_"), lit(floor(rand(100) * 10))))
    var df2 = rightTable
             .withColumn("newExplodedCol",
                          explode(array((0 to 10).map(lit(_)): _ *)))
    (df1, df2)   
}
    
   // val (newDf1, newDf2) = removeDataSkew(oldDf1, "_1", oldDf2)
    
    
    val df1 = spark.read.option("header", true).csv("input_data\\dataSkew")
    val df2 = df1.repartition(3, df1("id"))
    println("###########################Without Salting#####################################")
    df2.orderBy(spark_partition_id()).groupBy(spark_partition_id()).count().show()
    
    println("###############################################################################")
    
    println("#####################################After Salting#############################")
    val salt_df = spark.read.option("header", true).csv("input_data\\dataSkew")
    val salt_df1 = salt_df.withColumn("id_x", concat(salt_df("id"),lit("_"),lit(floor(rand(17000)*8))))
    salt_df1.repartition(3,salt_df1("id_x")).orderBy(spark_partition_id()).groupBy(spark_partition_id().alias("part_id"))
                  .count().show()
    //salt_df1.write.csv("output_data\\salted_vals")
  }
}