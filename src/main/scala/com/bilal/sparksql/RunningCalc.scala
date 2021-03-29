package com.bilal.sparksql
import org.apache.spark.sql.SparkSession
object RunningCalc {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("Run").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val in_df = spark.read.options(Map("header"->"true")).csv("input_data\\sql_inp\\runningcalc")
    println("The output dataframe is")
    in_df.show()
    in_df.createOrReplaceTempView("rundata")
    
    //Use case 1 : Find running total and running avg on entire table
    println("Running total and average on the entire table")
    spark.sql("""
              select *, sum(amount) over (
              order by id rows between unbounded preceding and current row 
              ) running_total,
              avg(amount) over(
              order by id rows between unbounded preceding and current row
              ) running_average from rundata
              """).show
              
    //Use case2: Find running total and average for each department
    println("Running total and avg on each department")
    spark.sql("""
              select *, sum(amount) over(
              partition by dept order by id rows between unbounded preceding and current row 
              ) running_total_dept,
              avg(amount) over(
              partition by dept order by id rows between unbounded preceding and current row
              ) running_avg_dept
              from rundata
              """).show    
    
     println("Unbounded preceding and unbounded following")
    spark.sql("""
              select *, sum(amount) over(
              partition by dept order by id rows between unbounded preceding and unbounded following 
              ) running_total_dept
              from rundata
              """).show 
              
    println("1 preceding and current row")
    spark.sql("""
              select *, sum(amount) over(
              partition by dept order by id rows between 1 preceding and current row 
              ) running_total_dept
              from rundata
              """).show
              
    println("current row and 1 following")
    spark.sql("""
              select *, sum(amount) over(
              partition by dept order by id rows between current row and 1 following 
              ) running_total_dept
              from rundata
              """).show
              
    println("current row and unbounded following")
    spark.sql("""
              select *, sum(amount) over(
              partition by dept order by id rows between current row and unbounded following 
              ) running_total_dept
              from rundata
              """).show
    
    println("Use lag function for amount for every id and department")
    spark.sql("""
              select *,lag(amount,1,0) over (partition by id,dept order by id) lag_amt
              from rundata
              """).show
    
    println("Use lead function for amount for every id and department")
    spark.sql("""
              select *,lead(amount,1,0) over (partition by id,dept order by id) lead_amt
              from rundata
              """).show
  }
}