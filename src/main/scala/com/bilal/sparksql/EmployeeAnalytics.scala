package com.bilal.sparksql
import org.apache.spark.sql.SparkSession
object EmployeeAnalytics {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("EmpAnalytics").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    //Employee Data
    
    val emp_df = spark.read.options(Map("header"->"true","sep"->"|")).csv("input_data\\sql_inp\\employee\\emp_data")
    emp_df.createOrReplaceTempView("empData")
    println("The input data is")
    val sel_emp_data ="select * from empData"
    val selEmp = spark.sql(sel_emp_data).show
    
    //Department Data
    val dept_df = spark.read.options(Map("header"->"true","sep"->"|")).csv("input_data\\sql_inp\\employee\\dept")
    dept_df.createOrReplaceTempView("deptData")
    val sel_dept_data ="select * from deptData"
    val selDep = spark.sql(sel_dept_data).show
    println("*******************************************************************************")
    
    //Use case 1: Find the most experienced person in each department. 
    //Output columns are --> emp id, emp name, Dept name in years
   
    println("Use case 1: Got the employee with most exp in each dept.")
   
    spark.sql(
             """select t.id emp_id,t.name emp_name,d.dept_name from
               (select id,name,dept_id,
               rank() over 
               (partition by dept_id order by datediff(current_date,to_date(from_unixtime(unix_timestamp(doj,'dd-MM-yyyy'), 'yyyy-MM-dd'))) desc) rank
               from empData) t join deptData d on t.dept_id == d.dept_id where t.rank = 1 order by t.id""").show
               
    //Use case 2: Find the employees who have more than 10 years of experience
    //Output columns are --> emp id, emp name, experience_in_years
     
     println("Use case 2: Finding no of employees with more than 10 yrs experience")
     
     spark.sql("""
               select emp_id,emp_name,experience from (select id emp_id,name emp_name,
               round(
               datediff(current_date,to_date(from_unixtime(unix_timestamp(doj,'dd-MM-yyyy'),'yyyy-MM-dd')))/365.25
               ) experience
               from empData) t
               where experience > 10
               """
              ).show
               
     //Use case 3: Display the department names alone which have employees more than 10 years of experience
      println("Departments which have employees more than 10 years of experience")
      
      spark.sql("""
                select b.dept_name from 
                (
                select dept_id from empData where 
                round(
                datediff(current_date,to_date(from_unixtime(unix_timestamp(doj,'dd-MM-yyyy'),'yyyy-MM-dd')))/365.25
                ) > 10 
                group by dept_id having count(id) > 0) a join deptData b on a.dept_id == b.dept_id
                """).show
                
       //Use case 4: Get the employee names as an array under same department
        spark.sql("""
                  select collect_set(name) names, dept_id from empData 
                  group by dept_id
                  """).show
  }
}