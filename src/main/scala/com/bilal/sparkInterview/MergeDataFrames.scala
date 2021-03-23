package com.bilal.sparkInterview
/**
 * This code will merge three dataframes which have different schemas into one single dataframe
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
object MergeDataFrames {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().master("local").appName("Merge df").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    var trade_df = spark.read.option("header", true)
                    .option("sep", "|").csv("G:\\Spark_Interview_inputs\\Merge dataframes\\trade")
    
    println("The trade DF is")
    trade_df.show(false)
    
    var position_df = spark.read.option("header", true).option("sep", "|")
                       .csv("G:\\Spark_Interview_inputs\\Merge dataframes\\positions")

    println("The position DF is")
    position_df.show(false)
    
    var asset_df = spark.read.option("header",true).option("sep", "|")
                   .csv("G:\\Spark_Interview_inputs\\Merge dataframes\\assets")

    println("The asset DF is")
    asset_df.show(false)
    
    // All the above 3 DFs have different schemas .
    //First need to merge two DFs Then using the merged data frame you will merge the third one.
    
    //First merging trade and position df
    //list1 will give the list of column of trade_df not found in position_df
    //list2 is vice versa of list1
    val list1 = trade_df.columns.toList.filterNot(position_df.columns.toSet)
    val list2 = position_df.columns.toList.filterNot(trade_df.columns.toSet)
    
    //add columns in list1 to position_df and from list2 add to trade_df 
    
    
    position_df = list1.foldLeft(position_df)((position_df,c)=>position_df.withColumn(c, lit(null).cast(StringType)))
    
    trade_df = list2.foldLeft(trade_df)((trade_df,c) => trade_df.withColumn(c, lit(null).cast(StringType)))
    
    //Columns have to be realigned or else union can go wrong
    //I am aligning position df as per trade df
    
    val trade_cols = trade_df.columns.toList
        
    position_df = position_df.select(trade_cols.map(position_df(_)): _*)
    
    var trade_pos = trade_df.unionAll(position_df)
    trade_pos.show(false)
    
    val list3 = trade_pos.columns.toList.filterNot(asset_df.columns.toSet)
    val list4 = asset_df.columns.toList.filterNot(trade_pos.columns.toSet)
    
    asset_df=list3.foldLeft(asset_df)((asset_df,c)=>asset_df.withColumn(c, lit(null).cast(StringType)))
    trade_pos = list4.foldLeft(trade_pos)((trade_pos,c)=>trade_pos.withColumn(c, lit(null).cast(StringType)))
    
    val fin_col = trade_pos.columns.toList 
    asset_df=asset_df.select(fin_col.map(asset_df(_)):_*)
    
    println("The final output is")
    val trade_pos_asset = trade_pos.unionAll(asset_df)
    trade_pos_asset.show(false)
    
  }
  
}