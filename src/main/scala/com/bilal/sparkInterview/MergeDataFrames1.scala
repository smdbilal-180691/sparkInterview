package com.bilal.sparkInterview
/**
 * This code will merge three dataframes which have different schemas into one single dataframe
 * The mapping of the three diff datafranes will be done to a common schema and union will be performed
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType,StructType}
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types.StructField

object MergeDataFrames1 {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("Merge df").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    def reader(spark:SparkSession,path:String):org.apache.spark.sql.DataFrame={
            spark.read.option("header", true)
                    .option("sep", "|").csv(path)
    }
    
    def list_of_cols(df:org.apache.spark.sql.DataFrame,fin_list:List[String])={
      fin_list.filterNot(df.columns.toSet)
    }
    
    def df_adjuster(df:org.apache.spark.sql.DataFrame,list:List[String],final_list:List[String])={
      val a = list
      .foldLeft(df)((df,c)=>df.withColumn(c, lit(null).cast(StringType)))
      
      a.select(final_list.map(a(_)):_*)
    }
    
    var trade_df = reader(spark,"input_Data\\merge_data\\trade")
    
    println("The trade DF is")
    trade_df.show(false)
    
    var position_df = reader(spark,"input_Data\\merge_data\\positions")

    println("The position DF is")
    position_df.show(false)
    
    var asset_df = reader(spark,"input_Data\\merge_data\\assets")

    println("The asset DF is")
    asset_df.show(false)
    
    //Final Schema is mail_id,number_of_shares,amount,trade_no,pos_no,share_name,asset_no,asset_name,asset_value
    
    val final_list = List("mail_id","number_of_shares","amount","trade_no","pos_no","share_name","asset_no","asset_name","asset_value")
    
    val trade_list = list_of_cols(trade_df, final_list)
    val position_list = list_of_cols(position_df, final_list)
    val asset_list = list_of_cols(asset_df, final_list)
    
    trade_df = df_adjuster(trade_df, trade_list,final_list)
    position_df = df_adjuster(position_df, position_list,final_list)
    asset_df = df_adjuster(asset_df, asset_list,final_list)
    
    trade_df.union(position_df).union(asset_df).show(false)
    
  }
}