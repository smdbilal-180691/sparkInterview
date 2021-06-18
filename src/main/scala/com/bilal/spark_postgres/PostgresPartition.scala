package com.bilal.spark_postgres
/**
 * This is used to obtain parallelism when u dont have incremental column value like id
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.spark_partition_id
object PostgresPartition {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().master("local").appName("Postgres").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val jdbcDF = spark.read.format("jdbc")
                  .option("url","jdbc:postgresql://localhost:5432/test" )
                  .option("dbtable","(select t1.*,row_number() over (order by ename) as num_rows from(select * from mailids) t1)tmp")
                  .option("numPartitions", 5)
                  .option("partitionColumn", "num_rows")
                  .option("upperBound", 15L)
                  .option("lowerBound", 1L)
                  .option("user","postgres")
                  .option("password","Allah.,123")
                  .load()
                  
                 jdbcDF.show
    jdbcDF.drop("num_rows").orderBy(spark_partition_id()).groupBy(spark_partition_id()).count().show()
    jdbcDF.drop("num_rows").select("*").withColumn("part_id", spark_partition_id()).show
  }
}