package com.bilal.sparkInterview
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
/**
 * This code will mask the phone number and email id column
 */
object DataMasking {
  def main(args:Array[String]):Unit={
    val spark =  SparkSession.builder().master("local").appName("DataMasking").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println(spark.version)
    
    val in_df = spark.read.option("header", "true")
                .csv("input_data\\data_masking")
    println("The input df is")
    in_df.show()
    
    def maskEmail(emailCol:String):String={
      val id = emailCol.split("@")(0).toCharArray()
      var res = ""
      for(i <- 0 to id.length-1){
        if(i==0 || i==id.length-1){res=res+id(i)}
        else{res=res+"*"}
      }
      res+"@"+emailCol.split("@")(1)
    }
    
    val maskEmail1 = udf((emailCol: String) => {
      var res = ""
      Option(emailCol) match{
        case Some(x) => {
          val id = x.split("@")(0).toCharArray()
          for(i <-0 to id.length-1){
            if(i==0 || i==id.length-1){res=res+id(i)}
            else{res=res+"*"}
          }
          res = s"${res}@${x.split("@")(1)}"
        }
        case None => res="No Email"
      }
      res
    })
    
    //println(maskEmail("smdbilal.vt5815@gmail.com"))
    
    
    def maskPhone2(phoneCol:String):String={
      var num = ""
      Option(phoneCol) match{
        case Some(x) => {
          val phone = x.toCharArray()
      
      for(i <-0 to phone.length-1){
        if(i==0 || i==phone.length-1){num=num+phone(i)}
        else{num=num+"*"}
        }
      }
        case None => num = "No Value"
      }
  num
    }
    
    println("1 "+maskPhone2("9791054432"))
    println("2 "+maskPhone2(null))
    
    val mask_email = spark.udf.register("mask_email_udf", maskEmail _)
    val mask_phone2 = spark.udf.register("mask_phone_udf2", maskPhone2 _)
    
    val out_df = in_df.withColumn("email", maskEmail1(in_df("email")))
                  .withColumn("mobile", mask_phone2(in_df("mobile")))
    
                  
    println("The output dataframe is")
    out_df.show(false)
  }
}